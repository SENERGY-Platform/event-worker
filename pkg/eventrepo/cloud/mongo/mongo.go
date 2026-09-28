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
	"context"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsontype"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"net/http"
	"reflect"
	"runtime/debug"
	"sync"
	"time"
)

type Mongo struct {
	config configuration.Config
	client *mongo.Client
	ctx    context.Context
}

var CreateCollections = []func(db *Mongo) error{}

var (
	errEmptyDatabase   = errors.New("mongo database name must not be empty")
	errMissingPassword = errors.New("mongo password must not be empty when a mongo user is set")
)

func New(ctx context.Context, wg *sync.WaitGroup, conf configuration.Config) (*Mongo, error) {
	if err := validateConfig(conf); err != nil {
		return nil, err
	}
	return start(ctx, wg, conf, clientOptions(conf), 10*time.Second)
}

// start disconnects the client on every failure path, so a failed startup leaves nothing connected.
func start(ctx context.Context, wg *sync.WaitGroup, conf configuration.Config, opts *options.ClientOptions, timeout time.Duration) (*Mongo, error) {
	client, err := connect(ctx, opts, conf.MongoDatabase, timeout)
	if err != nil {
		return nil, err
	}
	db := &Mongo{config: conf, client: client, ctx: ctx}
	for _, creators := range CreateCollections {
		err = creators(db)
		if err != nil {
			debug.PrintStack()
			disconnect(client, timeout)
			return nil, err
		}
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		log.Println("disconnect from mongo")
		disconnect(client, timeout)
	}()
	return db, nil
}

// connect runs listCollections on the service's database because Connect is lazy and ping needs no
// authentication; unreachable servers and wrong or missing credentials then fail at startup.
func connect(ctx context.Context, opts *options.ClientOptions, database string, timeout time.Duration) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	listOpts := options.ListCollections().SetNameOnly(true).SetAuthorizedCollections(true)
	if _, err = client.Database(database).ListCollectionNames(ctx, bson.D{}, listOpts); err != nil {
		disconnect(client, timeout)
		return nil, fmt.Errorf("mongo startup check failed: %w", err)
	}
	return client, nil
}

func disconnect(client *mongo.Client, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = client.Disconnect(ctx)
}

func validateConfig(conf configuration.Config) error {
	if conf.MongoDatabase == "" {
		return errEmptyDatabase
	}
	if conf.MongoUser != "" && conf.MongoPassword == "" {
		return errMissingPassword
	}
	return nil
}

// clientOptions applies the credentials after the URI so they replace any given in MONGO_URL.
func clientOptions(conf configuration.Config) *options.ClientOptions {
	reg := bson.NewRegistryBuilder().RegisterTypeMapEntry(bsontype.EmbeddedDocument, reflect.TypeOf(bson.M{})).Build() //ensure map marshalling to interface
	opts := options.Client().ApplyURI(conf.MongoUrl).SetRegistry(reg)
	if conf.MongoUser != "" {
		opts.SetAuth(options.Credential{
			Username:   conf.MongoUser,
			Password:   conf.MongoPassword,
			AuthSource: conf.MongoAuthSource,
		})
	}
	return opts
}

func (this *Mongo) getTimeoutContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(this.ctx, 10*time.Second)
}

func readCursorResult[T any](ctx context.Context, cursor *mongo.Cursor) (result []T, err error, code int) {
	result = []T{}
	for cursor.Next(ctx) {
		element := new(T)
		err = cursor.Decode(element)
		if err != nil {
			return result, err, http.StatusInternalServerError
		}
		result = append(result, *element)
	}
	err = cursor.Err()
	if err != nil {
		return result, err, http.StatusInternalServerError
	}
	return result, nil, http.StatusOK
}
