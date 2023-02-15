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
	"time"
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

	statMux           sync.Mutex
	statMsgCount      int
	statTopics        map[string]bool
	statAges          []int
	statEventRepoWait time.Duration
	statDoWait        time.Duration
	maxMsgAgeInSec    int
	statSkipCount     int
	statScriptCount   int
	statTriggerCount  int
}

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, eventRepo EventRepo, marshaller Marshaller, trigger Trigger, notifier Notifier) (w *Worker, err error) {
	w = &Worker{
		ctx:            ctx,
		wg:             wg,
		config:         config,
		eventRepo:      eventRepo,
		marshaller:     marshaller,
		trigger:        trigger,
		notifier:       notifier,
		work:           make(chan model.EventMessageDesc, config.ChannelSize),
		statTopics:     map[string]bool{},
		statAges:       []int{},
		maxMsgAgeInSec: -1,
	}
	if config.MaxMessageAge != "" && config.MaxMessageAge != "-" {
		maxAge, err := time.ParseDuration(config.MaxMessageAge)
		if err != nil {
			return w, err
		}
		w.maxMsgAgeInSec = int(maxAge.Seconds())
		log.Println("used maxMsgAgeInSec = ", w.maxMsgAgeInSec)
	}
	w.StartStatistics()
	w.startAsyncWorkers()
	return w, nil
}

type EventRepo interface {
	Get(message model.ConsumerMessage) (eventDesc []model.EventMessageDesc, err error)
}

type Marshaller interface {
	Unmarshal(desc model.EventMessageDesc) (value interface{}, err error)
}

type Trigger interface {
	Trigger(desc model.EventMessageDesc, value interface{}) error
}

type Notifier interface {
	NotifyError(desc model.EventMessageDesc, err error) error
}

func (this *Worker) Do(msg model.ConsumerMessage) error {
	this.logStats(msg.Topic, msg.AgeInSec)
	if this.maxMsgAgeInSec > 0 && msg.AgeInSec > this.maxMsgAgeInSec {
		this.logStatsSkip()
		return nil
	}
	start := time.Now()
	eventDesc, err := this.eventRepo.Get(msg)
	if err != nil {
		return err
	}
	this.logEventRepoWait(time.Since(start))
	defer this.logDoWait(time.Since(start))
	//handle qos=1 events first, to throw errors as early as possible
	wg := sync.WaitGroup{}
	for _, desc := range eventDesc {
		desc.MessageId = msg.MsgId
		desc.MessageAgeSeconds = msg.AgeInSec
		if desc.Qos == 1 {
			wg.Add(1)
			go func(desc model.EventMessageDesc) {
				defer wg.Done()
				tempErr := this.do(desc)
				if tempErr != nil {
					err = tempErr
				}
			}(desc)
		}
	}
	wg.Wait()
	if err != nil {
		return err
	}
	for _, desc := range eventDesc {
		desc.MessageId = msg.MsgId
		desc.MessageAgeSeconds = msg.AgeInSec
		if desc.Qos == 0 {
			this.work <- desc
		}
	}
	return nil
}

func (this *Worker) do(desc model.EventMessageDesc) error {
	value, err := this.marshaller.Unmarshal(desc)
	if err != nil {
		return this.handleError(err, desc)
	}
	this.logScript()
	trigger, err := this.evaluateScript(desc, value)
	if err != nil {
		return this.handleError(err, desc)
	}
	if trigger {
		this.logTrigger()
		err = this.trigger.Trigger(desc, value)
		if err != nil {
			return this.handleError(err, desc)
		}
	}
	return nil
}

func (this *Worker) handleError(err error, desc model.EventMessageDesc) error {
	if this.config.Debug {
		log.Println("ERROR:", err)
		debug.PrintStack()
	}
	if errors.Is(err, model.MessageIgnoreError) {
		notifierErr := this.notifier.NotifyError(desc, err)
		if notifierErr != nil {
			log.Println("ERROR:", notifierErr)
			debug.PrintStack()
		}
		return nil
	}
	return err
}

func (this *Worker) startAsyncWorkers() {
	for i := 0; i < this.config.ChannelWorkerCount; i++ {
		log.Println("start async do worker")
		this.wg.Add(1)
		go func() {
			defer this.wg.Done()
			defer log.Println("stop async do worker")
			for {
				select {
				case <-this.ctx.Done():
					return
				case work := <-this.work:
					err := this.do(work)
					if err != nil {
						log.Println("ERROR:", err)
					}
				}
			}
		}()
	}
}
