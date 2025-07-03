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

package pkg

import (
	"context"
	"github.com/SENERGY-Platform/event-worker/pkg/auth"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/consumer"
	"github.com/SENERGY-Platform/event-worker/pkg/devicerepo"
	"github.com/SENERGY-Platform/event-worker/pkg/eventrepo"
	"github.com/SENERGY-Platform/event-worker/pkg/marshaller"
	"github.com/SENERGY-Platform/event-worker/pkg/notifier"
	"github.com/SENERGY-Platform/event-worker/pkg/trigger"
	"github.com/SENERGY-Platform/event-worker/pkg/worker"
	"github.com/SENERGY-Platform/service-commons/pkg/cache/invalidator"
	"github.com/SENERGY-Platform/service-commons/pkg/kafka"
	"log"
	"runtime/debug"
	"sync"
	"time"
)

func Start(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) error {
	a := &auth.Auth{}

	repo, err := devicerepo.New(ctx, wg, config, a)
	if err != nil {
		return err
	}

	m, err := marshaller.New(ctx, wg, config, repo)
	if err != nil {
		return err
	}

	t, err := trigger.New(config, a)
	if err != nil {
		return err
	}

	n, err := notifier.New(ctx, wg, config)
	if err != nil {
		return err
	}

	e, err := eventrepo.New(ctx, wg, config, n)
	if err != nil {
		return err
	}

	w, err := worker.New(ctx, wg, config, e, m, t, n)
	if err != nil {
		return err
	}
	err = consumer.Start(ctx, wg, config, w)
	if err != nil {
		return err
	}
	if config.Mode == configuration.CloudMode {
		err = StartCacheInvalidator(ctx, config)
		if err != nil {
			log.Println("WARNING: unable to start cache invalidator", err)
			return nil
		}
	}
	return nil
}

func StartCacheInvalidator(ctx context.Context, conf configuration.Config) error {
	if conf.KafkaUrl == "" || conf.KafkaUrl == "-" {
		return nil
	}
	return invalidator.StartCacheInvalidatorAll(ctx, kafka.Config{
		KafkaUrl:               conf.KafkaUrl,
		StartOffset:            kafka.LastOffset,
		Debug:                  conf.Debug,
		PartitionWatchInterval: time.Minute,
		InitTopic:              conf.InitTopics,
		OnError: func(err error) {
			log.Println("ERROR:", err)
			debug.PrintStack()
		},
	}, conf.CacheInvalidationKafkaTopics, nil)
}
