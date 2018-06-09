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

package source_test

import (
	"fmt"
	"time"

	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/event"
	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/eventhandler"
	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/source"
	"github.com/kubernetes-sigs/controller-runtime/pkg/internal/informer/informertest"
	"github.com/kubernetes-sigs/controller-runtime/pkg/runtime/inject"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/workqueue"
)

var _ = Describe("Source", func() {
	Describe("KindSource", func() {
		var c chan struct{}
		var p *corev1.Pod
		var ic *informertest.FakeInformers

		BeforeEach(func() {
			ic = &informertest.FakeInformers{}
			c = make(chan struct{})
			p = &corev1.Pod{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{Name: "test", Image: "test"},
					},
				},
			}
		})

		Context("for a Pod resource", func() {
			It("should provide a Pod CreateEvent", func(done Done) {
				c := make(chan struct{})
				p := &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "test", Image: "test"},
						},
					},
				}

				q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
				instance := &source.KindSource{
					Type: &corev1.Pod{},
				}
				inject.DoInformers(ic, instance)
				err := instance.Start(eventhandler.Funcs{
					CreateFunc: func(q2 workqueue.RateLimitingInterface, evt event.CreateEvent) {
						defer GinkgoRecover()
						Expect(q2).To(Equal(q))
						Expect(evt.Meta).To(Equal(p))
						Expect(evt.Object).To(Equal(p))
						close(c)
					},
					UpdateFunc: func(workqueue.RateLimitingInterface, event.UpdateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected UpdateEvent")
					},
					DeleteFunc: func(workqueue.RateLimitingInterface, event.DeleteEvent) {
						defer GinkgoRecover()
						Fail("Unexpected DeleteEvent")
					},
					GenericFunc: func(workqueue.RateLimitingInterface, event.GenericEvent) {
						defer GinkgoRecover()
						Fail("Unexpected GenericEvent")
					},
				}, q)
				Expect(err).NotTo(HaveOccurred())

				i, err := ic.FakeInformerFor(&corev1.Pod{})
				Expect(err).NotTo(HaveOccurred())

				i.Add(p)
				<-c
				close(done)
			})

			It("should provide a Pod UpdateEvent", func(done Done) {
				p2 := p.DeepCopy()
				p2.SetLabels(map[string]string{"biz": "baz"})

				ic := &informertest.FakeInformers{}
				q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
				instance := &source.KindSource{
					Type: &corev1.Pod{},
				}
				instance.InjectInformers(ic)
				err := instance.Start(eventhandler.Funcs{
					CreateFunc: func(q2 workqueue.RateLimitingInterface, evt event.CreateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected CreateEvent")
					},
					UpdateFunc: func(q2 workqueue.RateLimitingInterface, evt event.UpdateEvent) {
						defer GinkgoRecover()
						Expect(q2).To(Equal(q))
						Expect(evt.MetaOld).To(Equal(p))
						Expect(evt.ObjectOld).To(Equal(p))

						Expect(evt.MetaNew).To(Equal(p2))
						Expect(evt.ObjectNew).To(Equal(p2))

						close(c)
					},
					DeleteFunc: func(workqueue.RateLimitingInterface, event.DeleteEvent) {
						defer GinkgoRecover()
						Fail("Unexpected DeleteEvent")
					},
					GenericFunc: func(workqueue.RateLimitingInterface, event.GenericEvent) {
						defer GinkgoRecover()
						Fail("Unexpected GenericEvent")
					},
				}, q)
				Expect(err).NotTo(HaveOccurred())

				i, err := ic.FakeInformerFor(&corev1.Pod{})
				Expect(err).NotTo(HaveOccurred())

				i.Update(p, p2)
				<-c
				close(done)
			})

			It("should provide a Pod DeletedEvent", func(done Done) {
				c := make(chan struct{})
				p := &corev1.Pod{
					Spec: corev1.PodSpec{
						Containers: []corev1.Container{
							{Name: "test", Image: "test"},
						},
					},
				}

				q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
				instance := &source.KindSource{
					Type: &corev1.Pod{},
				}
				inject.DoInformers(ic, instance)
				err := instance.Start(eventhandler.Funcs{
					CreateFunc: func(workqueue.RateLimitingInterface, event.CreateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected DeleteEvent")
					},
					UpdateFunc: func(workqueue.RateLimitingInterface, event.UpdateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected UpdateEvent")
					},
					DeleteFunc: func(q2 workqueue.RateLimitingInterface, evt event.DeleteEvent) {
						defer GinkgoRecover()
						Expect(q2).To(Equal(q))
						Expect(evt.Meta).To(Equal(p))
						Expect(evt.Object).To(Equal(p))
						close(c)
					},
					GenericFunc: func(workqueue.RateLimitingInterface, event.GenericEvent) {
						defer GinkgoRecover()
						Fail("Unexpected GenericEvent")
					},
				}, q)
				Expect(err).NotTo(HaveOccurred())

				i, err := ic.FakeInformerFor(&corev1.Pod{})
				Expect(err).NotTo(HaveOccurred())

				i.Delete(p)
				<-c
				close(done)
			})
		})
		Context("for a Kind not in the cache", func() {
			It("should return an error when Start is called", func(done Done) {
				ic.Error = fmt.Errorf("test error")
				q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")

				instance := &source.KindSource{
					Type: &corev1.Pod{},
				}
				instance.InjectInformers(ic)
				err := instance.Start(eventhandler.Funcs{}, q)
				Expect(err).To(HaveOccurred())

				close(done)
			})
		})
	})

	Describe("ChannelSource", func() {
		var stop chan struct{}
		var ch chan event.GenericEvent

		BeforeEach(func() {
			stop = make(chan struct{})
			ch = make(chan event.GenericEvent)
		})

		AfterEach(func() {
			close(stop)
			close(ch)
		})

		Context("for a source", func() {
			It("should provide a GenericEvent", func(done Done) {
				ch := make(chan event.GenericEvent)
				c := make(chan struct{})
				p := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "bar"},
				}
				evt := event.GenericEvent{
					Object: p,
					Meta:   p,
				}

				q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
				instance := &source.ChannelSource{Source: ch}
				inject.DoStop(stop, instance)
				err := instance.Start(eventhandler.Funcs{
					CreateFunc: func(workqueue.RateLimitingInterface, event.CreateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected CreateEvent")
					},
					UpdateFunc: func(workqueue.RateLimitingInterface, event.UpdateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected UpdateEvent")
					},
					DeleteFunc: func(workqueue.RateLimitingInterface, event.DeleteEvent) {
						defer GinkgoRecover()
						Fail("Unexpected DeleteEvent")
					},
					GenericFunc: func(q2 workqueue.RateLimitingInterface, evt event.GenericEvent) {
						defer GinkgoRecover()
						Expect(q2).To(Equal(q))
						Expect(evt.Meta).To(Equal(p))
						Expect(evt.Object).To(Equal(p))
						close(c)
					},
				}, q)
				Expect(err).NotTo(HaveOccurred())

				ch <- evt
				<-c
				close(done)
			})
			It("should block if exceed buffer size", func(done Done) {
				ch := make(chan event.GenericEvent)
				evt := event.GenericEvent{}
				interval := 5 * time.Second

				q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
				// Add a handler to get distribution blocked
				instance := &source.ChannelSource{Source: ch}
				instance.DestBufferSize = 1
				inject.DoStop(stop, instance)
				err := instance.Start(eventhandler.Funcs{
					CreateFunc: func(workqueue.RateLimitingInterface, event.CreateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected CreateEvent")
					},
					UpdateFunc: func(workqueue.RateLimitingInterface, event.UpdateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected UpdateEvent")
					},
					DeleteFunc: func(workqueue.RateLimitingInterface, event.DeleteEvent) {
						defer GinkgoRecover()
						Fail("Unexpected DeleteEvent")
					},
					GenericFunc: func(q2 workqueue.RateLimitingInterface, evt event.GenericEvent) {
						defer GinkgoRecover()
						time.Sleep(interval)
					},
				}, q)
				Expect(err).NotTo(HaveOccurred())

				// get channel blocked
				ch <- evt
				ch <- evt
				ch <- evt

				beforeEvent := time.Now()
				ch <- evt
				// validate event distribution get blocked.
				distributeInterval := time.Now().Sub(beforeEvent)
				Expect(distributeInterval >= interval).To(BeTrue())
				close(done)
			}, 15)
			It("should get error if no source specified", func(done Done) {
				q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
				instance := &source.ChannelSource{ /*no source specified*/ }
				inject.DoStop(stop, instance)
				err := instance.Start(eventhandler.Funcs{}, q)
				Expect(err).To(Equal(fmt.Errorf("must specify ChannelSource.Source")))
				close(done)
			})
			It("should get error if no stop channel injected", func(done Done) {
				q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
				instance := &source.ChannelSource{Source: ch}
				err := instance.Start(eventhandler.Funcs{}, q)
				Expect(err).To(Equal(fmt.Errorf("must call InjectStop on ChannelSource before calling Start")))
				close(done)
			})

		})
		Context("for multi sources (handlers)", func() {
			It("should provide GenericEvents for all handlers", func(done Done) {
				ch := make(chan event.GenericEvent)
				p := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "bar"},
				}
				evt := event.GenericEvent{
					Object: p,
					Meta:   p,
				}

				var resEvent1, resEvent2 event.GenericEvent
				c1 := make(chan struct{})
				c2 := make(chan struct{})

				q := workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "test")
				instance := &source.ChannelSource{Source: ch}
				inject.DoStop(stop, instance)
				err := instance.Start(eventhandler.Funcs{
					CreateFunc: func(workqueue.RateLimitingInterface, event.CreateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected CreateEvent")
					},
					UpdateFunc: func(workqueue.RateLimitingInterface, event.UpdateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected UpdateEvent")
					},
					DeleteFunc: func(workqueue.RateLimitingInterface, event.DeleteEvent) {
						defer GinkgoRecover()
						Fail("Unexpected DeleteEvent")
					},
					GenericFunc: func(q2 workqueue.RateLimitingInterface, evt event.GenericEvent) {
						defer GinkgoRecover()
						Expect(q2).To(Equal(q))
						Expect(evt.Meta).To(Equal(p))
						Expect(evt.Object).To(Equal(p))
						resEvent1 = evt
						close(c1)
					},
				}, q)
				Expect(err).NotTo(HaveOccurred())

				err = instance.Start(eventhandler.Funcs{
					CreateFunc: func(workqueue.RateLimitingInterface, event.CreateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected CreateEvent")
					},
					UpdateFunc: func(workqueue.RateLimitingInterface, event.UpdateEvent) {
						defer GinkgoRecover()
						Fail("Unexpected UpdateEvent")
					},
					DeleteFunc: func(workqueue.RateLimitingInterface, event.DeleteEvent) {
						defer GinkgoRecover()
						Fail("Unexpected DeleteEvent")
					},
					GenericFunc: func(q2 workqueue.RateLimitingInterface, evt event.GenericEvent) {
						defer GinkgoRecover()
						Expect(q2).To(Equal(q))
						Expect(evt.Meta).To(Equal(p))
						Expect(evt.Object).To(Equal(p))
						resEvent2 = evt
						close(c2)
					},
				}, q)
				Expect(err).NotTo(HaveOccurred())

				ch <- evt
				<-c1
				<-c2

				// Validate the two handlers received same event
				Expect(resEvent1).To(Equal(resEvent2))
				close(done)
			})
		})
	})
})
