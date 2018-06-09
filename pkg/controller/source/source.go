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

	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/event"
	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/eventhandler"
	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/source/internal"
	"github.com/kubernetes-sigs/controller-runtime/pkg/internal/informer"
	"github.com/kubernetes-sigs/controller-runtime/pkg/runtime/inject"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"

	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/predicate"
	logf "github.com/kubernetes-sigs/controller-runtime/pkg/runtime/log"
)

const (
	// defaultBufferSize is the default number of event notifications that can be buffered.
	defaultBufferSize = 1024
)

// Source is a source of events (eh.g. Create, Update, Delete operations on Kubernetes Objects, Webhook callbacks, etc)
// which should be processed by event.EventHandlers to enqueue reconcile.Requests.
//
// * Use KindSource for events originating in the cluster (eh.g. Pod Create, Pod Update, Deployment Update).
//
// * Use ChannelSource for events originating outside the cluster (eh.g. GitHub Webhook callback, Polling external urls).
//
// Users may build their own Source implementations.  If their implementations implement any of the inject package
// interfaces, the dependencies will be injected by the Controller when Watch is called.
type Source interface {
	// Start is internal and should be called only by the Controller to register an EventHandler with the Informer
	// to enqueue reconcile.Requests.
	Start(eventhandler.EventHandler, workqueue.RateLimitingInterface, ...predicate.Predicate) error
}

var _ Source = &ChannelSource{}

// ChannelSource is used to provide a source of events originating outside the cluster
// (eh.g. GitHub Webhook callback).  ChannelSource requires the user to wire the external
// source (eh.g. http handler) to write GenericEvents to the underlying channel.
type ChannelSource struct {
	// once ensures the event distribution goroutine will be performed only once
	once sync.Once

	// Source is the source channel to fetch GenericEvents
	Source <-chan event.GenericEvent

	// stop is to end ongoing goroutine, and close the channels
	stop <-chan struct{}

	// dest is the destination channels of the added event handlers
	dest []chan event.GenericEvent

	// DestBufferSize is the specified buffer size of dest channels.
	// Default to 1024 if not specified.
	DestBufferSize int

	// destLock is to ensure the destination channels are safely added/removed
	destLock sync.Mutex
}

var _ inject.Stop = &ChannelSource{}

// InjectStop is internal should be called only by the Controller.
// It is used to inject the stop channel initialized by the ControllerManager.
func (cs *ChannelSource) InjectStop(stop <-chan struct{}) error {
	if cs.stop == nil {
		cs.stop = stop
	}

	return nil
}

// Start implements Source and should only be called by the Controller.
func (cs *ChannelSource) Start(
	handler eventhandler.EventHandler,
	queue workqueue.RateLimitingInterface,
	prct ...predicate.Predicate) error {
	// Source should have been specified by the user.
	if cs.Source == nil {
		return fmt.Errorf("must specify ChannelSource.Source")
	}

	// stop should have been injected before Start was called
	if cs.stop == nil {
		return fmt.Errorf("must call InjectStop on ChannelSource before calling Start")
	}

	// use default value if DestBufferSize not specified
	if cs.DestBufferSize == 0 {
		cs.DestBufferSize = defaultBufferSize
	}

	cs.once.Do(func() {
		// Distribute GenericEvents to all EventHandler / Queue pairs Watching this source
		go cs.syncLoop()
	})

	dst := make(chan event.GenericEvent, cs.DestBufferSize)
	go func() {
		for evt := range dst {
			shouldHandle := true
			for _, p := range prct {
				if !p.Generic(evt) {
					shouldHandle = false
					break
				}
			}

			if shouldHandle {
				handler.Generic(queue, evt)
			}
		}
	}()

	cs.destLock.Lock()
	defer cs.destLock.Unlock()

	cs.dest = append(cs.dest, dst)

	return nil
}

func (cs *ChannelSource) doStop() {
	cs.destLock.Lock()
	defer cs.destLock.Unlock()

	for _, dst := range cs.dest {
		close(dst)
	}
}

func (cs *ChannelSource) distribute(evt event.GenericEvent) {
	cs.destLock.Lock()
	defer cs.destLock.Unlock()

	for _, dst := range cs.dest {
		// We cannot make it under goroutine here, or we'll meet the
		// race condition of writing message to closed channels.
		// To avoid blocking, the dest channels are expected to be of
		// proper buffer size. If we still see it blocked, then
		// the controller is thought to be in an abnormal state.
		dst <- evt
	}
}

func (cs *ChannelSource) syncLoop() {
	for {
		select {
		case <-cs.stop:
			// Close destination channels
			cs.doStop()
			return
		case evt := <-cs.Source:
			cs.distribute(evt)
		}
	}
}

var log = logf.KBLog.WithName("source").WithName("KindSource")

// KindSource is used to provide a source of events originating inside the cluster from Watches (eh.g. Pod Create)
type KindSource struct {
	// Type is the type of object to watch.  e.g. &v1.Pod{}
	Type runtime.Object

	// informers used to watch APIs
	informers informer.Informers
}

var _ Source = &KindSource{}

// Start is internal and should be called only by the Controller to register an EventHandler with the Informer
// to enqueue reconcile.Requests.
func (ks *KindSource) Start(handler eventhandler.EventHandler, queue workqueue.RateLimitingInterface,
	prct ...predicate.Predicate) error {

	// Type should have been specified by the user.
	if ks.Type == nil {
		return fmt.Errorf("must specify KindSource.Type")
	}

	// informers should have been injected before Start was called
	if ks.informers == nil {
		return fmt.Errorf("must call DoInformers on KindSource before calling Start")
	}

	// Lookup the Informer from the Informers and add an EventHandler which populates the Queue
	i, err := ks.informers.InformerFor(ks.Type)
	if err != nil {
		return err
	}
	i.AddEventHandler(internal.EventHandler{Queue: queue, EventHandler: handler, Predicates: prct})
	return nil
}

var _ inject.Informers = &KindSource{}

// InjectInformers is internal should be called only by the Controller.  InjectInformers is used to inject
// the Informers dependency initialized by the ControllerManager.
func (ks *KindSource) InjectInformers(i informer.Informers) error {
	if ks.informers == nil {
		ks.informers = i
	}
	return nil
}

// Func is a function that implements Source
type Func func(eventhandler.EventHandler, workqueue.RateLimitingInterface, ...predicate.Predicate) error

// Start implements Source
func (f Func) Start(evt eventhandler.EventHandler, queue workqueue.RateLimitingInterface,
	pr ...predicate.Predicate) error {
	return f(evt, queue, pr...)
}
