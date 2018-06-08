/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package source

import (
	"fmt"
	"sync"
	"time"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/buffer"
	"k8s.io/client-go/util/retry"
	"k8s.io/client-go/util/workqueue"

	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/event"
	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/eventhandler"
	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/source/internal"
	"github.com/kubernetes-sigs/controller-runtime/pkg/runtime/inject"

	logf "github.com/kubernetes-sigs/controller-runtime/pkg/runtime/log"
)

const (
	// initialBufferSize is the initial number of event notifications that can be buffered.
	initialBufferSize = 1024
)

var csLog = logf.KBLog.WithName("source").WithName("ChannelSource")

// ChannelSource is used to provide a source of events originating outside the cluster
// (eh.g. GitHub Webhook callback).  ChannelSource requires the user to wire the external
// source (eh.g. http handler) to write GenericEvents to the underlying channel.
type ChannelSource struct {
	informer *ChannelSourceInformer
}

var _ Source = &ChannelSource{}

// Start implements Source and should only be called by the Controller.
func (cs *ChannelSource) Start(
	handler eventhandler.EventHandler,
	queue workqueue.RateLimitingInterface) error {
	// informer should have been injected before Start was called
	if cs.informer == nil {
		return fmt.Errorf("must call InjectInformer on ChannelSource before calling Start")
	}

	cs.informer.AddEventHandler(internal.EventHandler{Queue: queue, EventHandler: handler})
	return nil
}

var _ inject.ChannelInformer = &ChannelSource{}

// InjectChannelInformer is internal should be called only by the Controller.
// InjectChannelInformer should be called before Start.
func (cs *ChannelSource) InjectChannelInformer(i *ChannelSourceInformer) error {
	if cs.informer == nil {
		cs.informer = i
	}
	return nil
}

// ChannelSourceInformer is a simplified informer basing on the SharedInformer in client-go.
// It fetch GenericEvent message from given channel, and distribute to registered handlers.
type ChannelSourceInformer struct {
	eventCh chan event.GenericEvent

	processor *sharedProcessor

	running bool
	startedLock sync.Mutex

	// blockDistribution gives a way to stop all event distribution so that a late event handler
	// can safely join the informer.
	blockDistribution sync.Mutex
}

// NewChannelSourceInformer creates a new instance for ChannelSourceInformer.
func NewChannelSourceInformer(eventCh chan event.GenericEvent) *ChannelSourceInformer {
	informer := &ChannelSourceInformer{
		eventCh: eventCh,
		processor: &sharedProcessor{},
	}
	return informer
}

// AddEventHandler register given handler to the informer.
func (i *ChannelSourceInformer) AddEventHandler(handler ChannelEventHandler) {
	i.startedLock.Lock()
	defer i.startedLock.Unlock()

	if !i.running {
		csLog.V(2).Info("Handler was not added to informer because it has stopped already")
		return
	}
	i.blockDistribution.Lock()
	defer i.blockDistribution.Unlock()

	listener := newProcessListener(handler, initialBufferSize)
	i.processor.addListener(listener)

}

func (i *ChannelSourceInformer) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	func() {
		i.startedLock.Lock()
		defer i.startedLock.Unlock()

		i.running = true
	}()

	processorStopCh := make(chan struct{})
	var wg wait.Group
	defer wg.Wait()              // Wait for Processor to stop
	defer close(processorStopCh) // Tell Processor to stop
	wg.StartWithChannel(processorStopCh, i.processor.run)

	defer func() {
		i.startedLock.Lock()
		defer i.startedLock.Unlock()
		i.running = false // Don't want any new listeners
	}()

	wait.Until(i.processLoop, time.Second, stopCh)
}

func (i *ChannelSourceInformer) processLoop() {
	for {
		e := <-i.eventCh
		i.blockDistribution.Lock()
		defer i.blockDistribution.Unlock()

		i.processor.distribute(e)
	}
}

type sharedProcessor struct {
	listenersStarted bool
	listenersLock    sync.RWMutex
	listeners        []*processorListener
	wg               wait.Group
}

func (p *sharedProcessor) addListener(listener *processorListener) {
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	p.addListenerLocked(listener)
	if p.listenersStarted {
		p.wg.Start(listener.run)
		p.wg.Start(listener.pop)
	}
}

func (p *sharedProcessor) addListenerLocked(listener *processorListener) {
	p.listeners = append(p.listeners, listener)
}

func (p *sharedProcessor) distribute(obj interface{}) {
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	for _, listener := range p.listeners {
		listener.add(obj)
	}
}

func (p *sharedProcessor) run(stopCh <-chan struct{}) {
	func() {
		p.listenersLock.RLock()
		defer p.listenersLock.RUnlock()
		for _, listener := range p.listeners {
			p.wg.Start(listener.run)
			p.wg.Start(listener.pop)
		}
		p.listenersStarted = true
	}()
	<-stopCh
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()
	for _, listener := range p.listeners {
		close(listener.addCh) // Tell .pop() to stop. .pop() will tell .run() to stop
	}
	p.wg.Wait() // Wait for all .pop() and .run() to stop
}

type processorListener struct {
	nextCh chan interface{}
	addCh  chan interface{}

	handler ChannelEventHandler

	// pendingNotifications is an unbounded ring buffer that holds all notifications not yet distributed.
	// There is one per listener, but a failing/stalled listener will have infinite pendingNotifications
	// added until we OOM.
	pendingNotifications buffer.RingGrowing
}

func newProcessListener(handler ChannelEventHandler, bufferSize int) *processorListener {
	ret := &processorListener{
		nextCh:                make(chan interface{}),
		addCh:                 make(chan interface{}),
		handler:               handler,
		pendingNotifications:  *buffer.NewRingGrowing(bufferSize),
	}

	return ret
}

func (p *processorListener) add(notification interface{}) {
	p.addCh <- notification
}

func (p *processorListener) pop() {
	defer utilruntime.HandleCrash()
	defer close(p.nextCh) // Tell .run() to stop

	var nextCh chan<- interface{}
	var notification interface{}
	for {
		select {
		case nextCh <- notification:
			// Notification dispatched
			var ok bool
			notification, ok = p.pendingNotifications.ReadOne()
			if !ok { // Nothing to pop
				nextCh = nil // Disable this select case
			}
		case notificationToAdd, ok := <-p.addCh:
			if !ok {
				return
			}
			if notification == nil { // No notification to pop (and pendingNotifications is empty)
				// Optimize the case - skip adding to pendingNotifications
				notification = notificationToAdd
				nextCh = p.nextCh
			} else { // There is already a notification waiting to be dispatched
				p.pendingNotifications.WriteOne(notificationToAdd)
			}
		}
	}
}

func (p *processorListener) run() {
	// this call blocks until the channel is closed.  When a panic happens during the notification
	// we will catch it, **the offending item will be skipped!**, and after a short delay (one second)
	// the next notification will be attempted.  This is usually better than the alternative of never
	// delivering again.
	stopCh := make(chan struct{})
	wait.Until(func() {
		// this gives us a few quick retries before a long pause and then a few more quick retries
		err := wait.ExponentialBackoff(retry.DefaultRetry, func() (bool, error) {
			for next := range p.nextCh {
				switch notification := next.(type) {
				case event.GenericEvent:
					p.handler.OnGeneric(notification)
				default:
					utilruntime.HandleError(fmt.Errorf("unrecognized notification: %#v", next))
				}
			}
			// the only way to get here is if the p.nextCh is empty and closed
			return true, nil
		})

		// the only way to get here is if the p.nextCh is empty and closed
		if err == nil {
			close(stopCh)
		}
	}, 1*time.Minute, stopCh)
}

// ChannelEventHandler can handle notifications in format of GeneticEvent.
// The events are informational only, so you can't return an error.
type ChannelEventHandler interface {
	OnGeneric(obj interface{})
}
