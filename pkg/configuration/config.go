/*
 * Copyright (c) 2022-2023 InfAI (CC SES)
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

package configuration

import (
	"encoding/json"
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	Debug bool `json:"debug"`
	Mode  Mode `json:"mode"`

	ChannelSize        int `json:"channel_size"`
	ChannelWorkerCount int `json:"channel_worker_count"`

	MaxMessageAge string `json:"max_message_age"`

	AuthEndpoint             string  `json:"auth_endpoint"`
	AuthClientId             string  `json:"auth_client_id"`
	AuthExpirationTimeBuffer float64 `json:"auth_expiration_time_buffer"`

	DeviceRepoUrl           string `json:"device_repo_url"`
	DeviceRepoCacheDuration string `json:"device_repo_cache_duration"`

	NotificationUrl                            string `json:"notification_url"`
	NotificationsIgnoreDuplicatesWithinSeconds string `json:"notifications_ignore_duplicates_within_seconds"`

	EventTriggerUrl string `json:"event_trigger_url"` // Cloud: http://engine-wrapper-url:8080/v2/event-trigger or Fog: http://local-camunda-url:8080/engine-rest/message

	//fog
	MgwMqttPw       string `json:"mgw_mqtt_pw"`
	MgwMqttUser     string `json:"mgw_mqtt_user"`
	MgwMqttClientId string `json:"mgw_mqtt_client_id"`
	MgwMqttBroker   string `json:"mgw_mqtt_broker"`
	MgwMqttQos      byte   `json:"mgw_mqtt_qos"`

	AuthUserName string `json:"auth_user_name"`
	AuthPassword string `json:"auth_password"`

	FallbackFile string `json:"fallback_file"`

	//cloud
	KafkaUrl               string              `json:"kafka_url"`
	KafkaConsumerGroup     string              `json:"kafka_consumer_group"`
	DeviceTypeTopic        string              `json:"device_type_topic"`
	ProcessDeploymentTopic string              `json:"process_deployment_topic"`
	KafkaTopicSliceCount   int                 `json:"kafka_topic_slice_count"`
	KafkaTopicSliceIndex   int                 `json:"kafka_topic_slice_index"`
	InstanceId             string              `json:"instance_id"`
	ServiceTopicConfig     []kafka.ConfigEntry `json:"service_topic_config"`

	CloudEventRepoCacheDuration       string `json:"cloud_event_repo_cache_duration"`
	CloudEventRepoMongoUrl            string `json:"cloud_event_repo_mongo_url"`
	CloudEventRepoMongoTable          string `json:"cloud_event_repo_mongo_table"`
	CloudEventRepoMongoDescCollection string `json:"cloud_event_repo_mongo_desc_collection"`

	AuthClientSecret string `json:"auth_client_secret"`

	//all
	FatalErrHandler func(v ...interface{})
}

type Mode = string

var FogMode Mode = "fog"
var CloudMode Mode = "cloud"

func (this Config) HandleFatalError(v ...interface{}) {
	if this.FatalErrHandler != nil {
		this.FatalErrHandler(v...)
	} else {
		log.Fatal(v...)
	}
}

// loads config from json in location and used environment variables (e.g KafkaUrl --> KAFKA_URL)
func Load(location string) (config Config, err error) {
	file, err := os.Open(location)
	if err != nil {
		return config, err
	}
	err = json.NewDecoder(file).Decode(&config)
	if err != nil {
		return config, err
	}
	handleEnvironmentVars(&config)
	handleInstanceIdAsSliceIndex(&config)
	return config, nil
}

var camel = regexp.MustCompile("(^[^A-Z]*|[A-Z]*)([A-Z][^A-Z]+|$)")

func fieldNameToEnvName(s string) string {
	var a []string
	for _, sub := range camel.FindAllStringSubmatch(s, -1) {
		if sub[1] != "" {
			a = append(a, sub[1])
		}
		if sub[2] != "" {
			a = append(a, sub[2])
		}
	}
	return strings.ToUpper(strings.Join(a, "_"))
}

// preparations for docker
func handleEnvironmentVars(config *Config) {
	configValue := reflect.Indirect(reflect.ValueOf(config))
	configType := configValue.Type()
	for index := 0; index < configType.NumField(); index++ {
		fieldName := configType.Field(index).Name
		fieldConfig := configType.Field(index).Tag.Get("config")
		envName := fieldNameToEnvName(fieldName)
		envValue := os.Getenv(envName)
		if envValue != "" {
			if !strings.Contains(fieldConfig, "secret") {
				fmt.Println("use environment variable: ", envName, " = ", envValue)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Int64 || configValue.FieldByName(fieldName).Kind() == reflect.Int {
				i, _ := strconv.ParseInt(envValue, 10, 64)
				configValue.FieldByName(fieldName).SetInt(i)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.String {
				configValue.FieldByName(fieldName).SetString(envValue)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Bool {
				b, _ := strconv.ParseBool(envValue)
				configValue.FieldByName(fieldName).SetBool(b)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Float64 {
				f, _ := strconv.ParseFloat(envValue, 64)
				configValue.FieldByName(fieldName).SetFloat(f)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Slice {
				val := []string{}
				for _, element := range strings.Split(envValue, ",") {
					val = append(val, strings.TrimSpace(element))
				}
				configValue.FieldByName(fieldName).Set(reflect.ValueOf(val))
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Map {
				value := map[string]string{}
				for _, element := range strings.Split(envValue, ",") {
					keyVal := strings.Split(element, ":")
					key := strings.TrimSpace(keyVal[0])
					val := strings.TrimSpace(keyVal[1])
					value[key] = val
				}
				configValue.FieldByName(fieldName).Set(reflect.ValueOf(value))
			}
		}
	}
}
