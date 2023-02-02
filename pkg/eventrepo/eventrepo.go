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

package eventrepo

import (
	"github.com/SENERGY-Platform/event-worker/pkg/model"
)

type EventRepo struct{}

func (this *EventRepo) Get(message model.ConsumerMessage) (eventDesc []model.EventMessageDesc, err error) {
	return nil, nil
	//TODO implement me
	panic("implement me")
}
