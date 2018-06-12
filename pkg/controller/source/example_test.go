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
	"github.com/kubernetes-sigs/controller-runtime/pkg/controller"
	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/event"
	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/eventhandler"
	"github.com/kubernetes-sigs/controller-runtime/pkg/controller/source"
	"k8s.io/api/core/v1"
)

var ctrl controller.Controller

// This example Watches for Pod Events (e.g. Create / Update / Delete) and enqueues a reconcile.Request
// with the Name and Namespace of the Pod.
func ExampleKindSource() {
	ctrl.Watch(
		&source.KindSource{Type: &v1.Pod{}},
		&eventhandler.EnqueueHandler{},
	)
}

// This example reads GenericEvents from a channel and enqueues a reconcile.Request containing the Name and Namespace
// provided by the event.
func ExampleChannelSource() {
	events := make(chan event.GenericEvent)

	ctrl.Watch(
		&source.ChannelSource{Source: events},
		&eventhandler.EnqueueHandler{},
	)
}
