/*
 * Copyright (c) 2026 InfAI (CC SES)
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
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
)

// Fog mode reads descriptions over HTTP, so it must start with no usable Mongo settings at all.
func TestFogModeStartsWithoutMongoSettings(t *testing.T) {
	config, err := configuration.Load("../../config.json")
	if err != nil {
		t.Fatal(err)
	}
	config.Mode = configuration.FogMode
	config.MongoUrl = ""
	config.MongoDatabase = ""
	config.MongoAuthSource = ""
	config.MongoUser = "user-without-password"
	config.MongoPassword = ""
	config.CloudEventRepoMongoDescCollection = ""

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	if _, err = New(ctx, wg, config, nil); err != nil {
		t.Fatalf("fog mode touched the Mongo settings: %v", err)
	}
	cancel()
	wg.Wait()

	// The same settings in cloud mode are rejected, so the fog result is not an accident of them.
	config.Mode = configuration.CloudMode
	_, err = New(context.Background(), &sync.WaitGroup{}, config, nil)
	if err == nil || !strings.Contains(err.Error(), "mongo database name must not be empty") {
		t.Fatalf("cloud mode err = %v, want the empty-database validation error", err)
	}
}
