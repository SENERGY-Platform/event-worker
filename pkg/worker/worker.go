/*
 * Copyright (c) 2023 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package worker

import (
	"context"
	"errors"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"log"
	"runtime/debug"
	"sync"
)

type Worker struct {
	ctx    context.Context
	wg     *sync.WaitGroup
	config configuration.Config

	eventRepo  EventRepo
	marshaller Marshaller
	trigger    Trigger
	notifier   Notifier

	work chan model.EventMessageDesc

	statMux      sync.Mutex
	statMsgCount int
	statTopics   map[string]bool
}

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, eventRepo EventRepo, marshaller Marshaller, trigger Trigger, notifier Notifier) (w *Worker) {
	w = &Worker{
		ctx:        ctx,
		wg:         wg,
		config:     config,
		eventRepo:  eventRepo,
		marshaller: marshaller,
		trigger:    trigger,
		notifier:   notifier,
		work:       make(chan model.EventMessageDesc, config.ChannelSize),
		statTopics: map[string]bool{},
	}
	w.StartStatistics()
	w.startAsyncWorkers()
	return w
}

type EventRepo interface {
	Get(topic string, message []byte) (eventDesc []model.EventMessageDesc, err error)
}

type Marshaller interface {
	Unmarshal(desc model.EventMessageDesc) (value interface{}, err error)
}

type Trigger interface {
	Trigger(desc model.EventMessageDesc, value interface{}) error
}

type Notifier interface {
	NotifyError(desc model.EventMessageDesc, err error)
}

func (this *Worker) Do(topic string, message []byte) error {
	this.logStats(topic)
	eventDesc, err := this.eventRepo.Get(topic, message)
	if err != nil {
		return err
	}
	for _, desc := range eventDesc {
		if desc.Qos == 0 {
			this.work <- desc
		} else {
			err = this.do(desc)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (this *Worker) do(desc model.EventMessageDesc) error {
	value, err := this.marshaller.Unmarshal(desc)
	if err != nil {
		return this.handleError(err, desc)
	}
	trigger, err := this.evaluateScript(desc, value)
	if err != nil {
		return this.handleError(err, desc)
	}
	if trigger {
		err = this.trigger.Trigger(desc, value)
		if err != nil {
			return this.handleError(err, desc)
		}
	}
	return nil
}

func (this *Worker) handleError(err error, desc model.EventMessageDesc) error {
	if errors.Is(err, model.MessageIgnoreError) {
		if this.config.Debug {
			log.Println("ERROR:", err)
			debug.PrintStack()
		}
		this.notifier.NotifyError(desc, err)
		return nil
	}
	return err
}

func (this *Worker) startAsyncWorkers() {
	for i := 0; i < this.config.ChannelWorkerCount; i++ {
		this.wg.Add(1)
		go func() {
			defer this.wg.Done()
			select {
			case <-this.ctx.Done():
				return
			case work := <-this.work:
				err := this.do(work)
				if err != nil {
					log.Println("ERROR:", err)
				}
			}
		}()
	}
}
