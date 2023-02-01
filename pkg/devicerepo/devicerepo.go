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

package devicerepo

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/SENERGY-Platform/event-worker/pkg/auth"
	"github.com/SENERGY-Platform/event-worker/pkg/cache"
	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/marshaller/lib/marshaller/model"
	"github.com/SENERGY-Platform/models/go/models"
	"io"
	"log"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, auth *auth.Auth) (result *DeviceRepo, err error) {
	result = &DeviceRepo{
		auth: auth,
	}
	cacheDuration, err := time.ParseDuration(config.CacheDuration)
	if err != nil {
		return result, err
	}
	switch config.Mode {
	case configuration.FogMode:
		fallback, err := cache.NewFallback(config.FallbackFile)
		if err != nil {
			return result, err
		}
		result.cache = cache.NewCacheWithFallback(int(cacheDuration.Seconds()), fallback)
	case configuration.CloudMode:
		result.cache = cache.NewCache(int(cacheDuration.Seconds()))
	default:
		return nil, errors.New("unknown mode: " + config.Mode)
	}
	return result, nil
}

type DeviceRepo struct {
	auth   *auth.Auth
	cache  *cache.Cache
	config configuration.Config
}

func (this *DeviceRepo) GetJson(token string, endpoint string, result interface{}) (err error) {
	req, err := http.NewRequest("GET", endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", token)
	client := &http.Client{
		Timeout: 10 * time.Second,
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		temp, _ := io.ReadAll(resp.Body)
		return errors.New(strings.TrimSpace(string(temp)))
	}
	err = json.NewDecoder(resp.Body).Decode(result)
	if err != nil {
		log.Println("ERROR:", err.Error())
		debug.PrintStack()
	}
	return
}

func (this *DeviceRepo) getToken() (string, error) {
	if this.auth == nil {
		this.auth = &auth.Auth{}
	}
	return this.auth.EnsureAccess(this.config)
}

func (this *DeviceRepo) GetCharacteristic(id string) (result models.Characteristic, err error) {
	err = this.cache.Use("characteristics."+id, func() (interface{}, error) {
		return this.getCharacteristic(id)
	}, &result)
	return
}

func (this *DeviceRepo) getCharacteristic(id string) (result models.Characteristic, err error) {
	token, err := this.getToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepoUrl+"/characteristics/"+url.PathEscape(id), &result)
	return
}

func (this *DeviceRepo) GetConcept(id string) (result models.Concept, err error) {
	err = this.cache.Use("concept."+id, func() (interface{}, error) {
		return this.getConcept(id)
	}, &result)
	return
}

func (this *DeviceRepo) getConcept(id string) (result model.Concept, err error) {
	token, err := this.getToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepoUrl+"/concepts/"+url.PathEscape(id), &result)
	return
}

func (this *DeviceRepo) GetConceptIdOfFunction(id string) string {
	//TODO implement me
	panic("implement me")
}

func (this *DeviceRepo) GetAspectNode(id string) (result models.AspectNode, err error) {
	err = this.cache.Use("aspect-nodes."+id, func() (interface{}, error) {
		return this.getAspectNode(id)
	}, &result)
	return
}

func (this *DeviceRepo) getAspectNode(id string) (result models.AspectNode, err error) {
	token, err := this.getToken()
	if err != nil {
		return result, err
	}
	err = this.GetJson(token, this.config.DeviceRepoUrl+"/aspect-nodes/"+url.QueryEscape(id), &result)
	return
}
