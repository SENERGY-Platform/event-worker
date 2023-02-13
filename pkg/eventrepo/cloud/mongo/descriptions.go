/*
 * Copyright (c) 2022 InfAI (CC SES)
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

package mongo

import (
	"errors"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"runtime/debug"
)

var DescBson = getBsonFieldObject[model.EventDesc]()

var ErrReleaseNotFound = errors.New("release not found")

func init() {
	CreateCollections = append(CreateCollections, func(db *Mongo) error {
		var err error
		collection := db.client.Database(db.config.CloudEventRepoMongoTable).Collection(db.config.CloudEventRepoMongoDescCollection)
		err = db.ensureCompoundIndex(collection, "event_desc_device_service_index", true, false, DescBson.DeviceId, DescBson.ServiceId)
		if err != nil {
			debug.PrintStack()
			return err
		}
		err = db.ensureIndex(collection, "event_desc_import_index", DescBson.ImportId, true, false)
		if err != nil {
			debug.PrintStack()
			return err
		}
		err = db.ensureIndex(collection, "event_desc_deployment_index", DescBson.DeploymentId, true, false)
		if err != nil {
			debug.PrintStack()
			return err
		}
		err = db.ensureIndex(collection, "event_desc_group_index", DescBson.DeviceGroupId, true, false)
		if err != nil {
			debug.PrintStack()
			return err
		}
		err = db.ensureIndex(collection, "event_desc_service_index", DescBson.ServiceId, true, false)
		if err != nil {
			debug.PrintStack()
			return err
		}
		return nil
	})
}

func (this *Mongo) descCollection() *mongo.Collection {
	return this.client.Database(this.config.CloudEventRepoMongoTable).Collection(this.config.CloudEventRepoMongoDescCollection)
}

func (this *Mongo) GetEventDescriptionsByImportId(importId string) (result []model.EventDesc, err error) {
	if importId == "" {
		return []model.EventDesc{}, nil
	}
	ctx, _ := this.getTimeoutContext()
	cursor, err := this.descCollection().Find(ctx, bson.M{DescBson.ImportId: importId})
	if err != nil {
		return result, err
	}
	result, err, _ = readCursorResult[model.EventDesc](ctx, cursor)
	return result, err
}

func (this *Mongo) GetEventDescriptionsByDeviceAndService(deviceId string, serviceId string) (result []model.EventDesc, err error) {
	if deviceId == "" || serviceId == "" {
		return []model.EventDesc{}, nil
	}
	ctx, _ := this.getTimeoutContext()
	cursor, err := this.descCollection().Find(ctx, bson.M{DescBson.DeviceId: deviceId, DescBson.ServiceId: serviceId})
	if err != nil {
		debug.PrintStack()
		return result, err
	}
	result, err, _ = readCursorResult[model.EventDesc](ctx, cursor)
	return result, err
}

func (this *Mongo) GetEventDescriptionsByDeviceGroup(deviceGroupId string) (result []model.EventDesc, err error) {
	if deviceGroupId == "" {
		return []model.EventDesc{}, nil
	}
	ctx, _ := this.getTimeoutContext()
	cursor, err := this.descCollection().Find(ctx, bson.M{DescBson.DeviceGroupId: deviceGroupId})
	if err != nil {
		debug.PrintStack()
		return result, err
	}
	result, err, _ = readCursorResult[model.EventDesc](ctx, cursor)
	return result, err
}

func (this *Mongo) GetEventDescriptionsByServiceId(serviceId string) (result []model.EventDesc, err error) {
	if serviceId == "" {
		return []model.EventDesc{}, nil
	}
	ctx, _ := this.getTimeoutContext()
	cursor, err := this.descCollection().Find(ctx, bson.M{DescBson.ServiceId: serviceId})
	if err != nil {
		debug.PrintStack()
		return result, err
	}
	result, err, _ = readCursorResult[model.EventDesc](ctx, cursor)
	return result, err
}

func (this *Mongo) GetEventDescriptionsByEventId(eventId string) (result []model.EventDesc, err error) {
	if eventId == "" {
		return []model.EventDesc{}, nil
	}
	ctx, _ := this.getTimeoutContext()
	cursor, err := this.descCollection().Find(ctx, bson.M{DescBson.EventId: eventId})
	if err != nil {
		debug.PrintStack()
		return result, err
	}
	result, err, _ = readCursorResult[model.EventDesc](ctx, cursor)
	return result, err
}

func (this *Mongo) RemoveEventDescriptionsByDeploymentId(deploymentId string) (err error) {
	ctx, _ := this.getTimeoutContext()
	_, err = this.descCollection().DeleteMany(ctx, bson.M{DescBson.DeploymentId: deploymentId})
	return err
}

func (this *Mongo) SetEventDescription(element model.EventDesc) (err error) {
	if element.DeploymentId == "" {
		return errors.New("missing deployment id")
	}
	if element.EventId == "" {
		return errors.New("missing event id")
	}
	ctx, _ := this.getTimeoutContext()
	_, err = this.descCollection().InsertOne(ctx, element)
	return err
}
