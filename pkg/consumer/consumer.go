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

package consumer

import (
	"context"
	"errors"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/consumer/cloud"
	"github.com/SENERGY-Platform/event-worker/pkg/consumer/fog"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"sync"
)

type Worker interface {
	Do(message model.ConsumerMessage) error
	ResetCache()
}

func Start(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, worker Worker) error {
	switch config.Mode {
	case configuration.FogMode:
		return fog.Start(ctx, wg, config, worker)
	case configuration.CloudMode:
		return cloud.Start(ctx, wg, config, worker)
	default:
		return errors.New("unknown mode: " + config.Mode)
	}
}
