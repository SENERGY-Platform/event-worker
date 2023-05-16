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

package cloud

import (
	"context"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/topics"
	"io"
	"log"
	"net"
	"os"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
)

func NewKafkaLastOffsetConsumerGroup(ctx context.Context, wg *sync.WaitGroup, broker string, groupId string, topics []string, listener func(msg model.ConsumerMessage) error, errhandler func(topic string, err error)) error {
	if len(topics) == 0 {
		return nil
	}
	if len(topics) > 20 {
		log.Println("consume:", len(topics), "topics")
	} else {
		log.Println("consume:", topics)
	}

	r := kafka.NewReader(kafka.ReaderConfig{
		StartOffset:            kafka.LastOffset,
		GroupBalancers:         []kafka.GroupBalancer{kafka.RoundRobinGroupBalancer{}, kafka.RangeGroupBalancer{}},
		CommitInterval:         0, //synchronous commits
		Brokers:                []string{broker},
		GroupID:                groupId,
		GroupTopics:            topics,
		Logger:                 log.New(io.Discard, "", 0),
		ErrorLogger:            log.New(os.Stdout, "[KAFKA-ERROR] ", log.Default().Flags()),
		WatchPartitionChanges:  true,
		PartitionWatchInterval: time.Minute,
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer r.Close()
		defer func() {
			if len(topics) > 20 {
				log.Println("close consumer for", len(topics), "topics")
			} else {
				log.Println("close consumer for topics ", topics)
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := r.FetchMessage(ctx)
				if err == io.EOF || err == context.Canceled {
					return
				}
				topic := m.Topic
				if err != nil {
					log.Println("ERROR: while consuming topic ", topic, err)
					errhandler(topic, err)
					return
				}

				err = retry(func() error {
					return listener(model.ConsumerMessage{
						Topic:    topic,
						Message:  m.Value,
						AgeInSec: int(time.Since(m.Time).Seconds()),
						MsgId:    fmt.Sprintf("%v:%v", m.Partition, m.Offset),
					})
				}, func(n int64) time.Duration {
					return time.Duration(n) * time.Second
				}, 10*time.Minute)

				if err != nil {
					log.Println("ERROR: unable to handle message (no commit)", err)
					errhandler(topic, err)
				} else {
					err = r.CommitMessages(ctx, m)
				}
			}
		}
	}()
	return nil
}

func NewKafkaLastOffsetConsumer(ctx context.Context, wg *sync.WaitGroup, broker string, groupId string, topic string, listener func(delivery []byte) error, errhandler func(err error)) error {
	log.Println("consume:", topic)

	r := kafka.NewReader(kafka.ReaderConfig{
		StartOffset:            kafka.LastOffset,
		GroupID:                groupId,
		CommitInterval:         0, //synchronous commits
		Brokers:                []string{broker},
		Topic:                  topic,
		Logger:                 log.New(io.Discard, "", 0),
		ErrorLogger:            log.New(os.Stdout, "[KAFKA-ERROR] ", log.Default().Flags()),
		WatchPartitionChanges:  true,
		PartitionWatchInterval: time.Minute,
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer r.Close()
		defer log.Println("close consumer for topic ", topic)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m, err := r.FetchMessage(ctx)
				if err == io.EOF || err == context.Canceled {
					return
				}
				topic := m.Topic
				if err != nil {
					log.Println("ERROR: while consuming topic ", topic, err)
					errhandler(err)
					return
				}

				err = retry(func() error {
					return listener(m.Value)
				}, func(n int64) time.Duration {
					return time.Duration(n) * time.Second
				}, 10*time.Minute)

				if err != nil {
					log.Println("ERROR: unable to handle message (no commit)", err)
					errhandler(err)
				} else {
					err = r.CommitMessages(ctx, m)
				}
			}
		}
	}()
	return nil
}

func retry(f func() error, waitProvider func(n int64) time.Duration, timeout time.Duration) (err error) {
	err = errors.New("initial")
	start := time.Now()
	for i := int64(1); err != nil && time.Since(start) < timeout; i++ {
		err = f()
		if err != nil {
			log.Println("ERROR: kafka listener error:", err)
			wait := waitProvider(i)
			if time.Since(start)+wait < timeout {
				log.Println("ERROR: retry after:", wait.String())
				time.Sleep(wait)
			} else {
				return err
			}
		}
	}
	return err
}

func getBroker(bootstrapUrl string) (result []string, err error) {
	conn, err := kafka.Dial("tcp", bootstrapUrl)
	if err != nil {
		return result, err
	}
	defer conn.Close()
	brokers, err := conn.Brokers()
	if err != nil {
		return result, err
	}
	for _, broker := range brokers {
		result = append(result, net.JoinHostPort(broker.Host, strconv.Itoa(broker.Port)))
	}
	return result, nil
}

func InitTopics(bootstrapUrl string, config []kafka.ConfigEntry, topics ...string) (err error) {
	log.Println("init topics", topics)
	conn, err := kafka.Dial("tcp", bootstrapUrl)
	if err != nil {
		return err
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{}

	for _, topic := range topics {
		topicConfigs = append(topicConfigs, kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
			ConfigEntries:     config,
		})
	}

	return controllerConn.CreateTopics(topicConfigs...)
}

func GetTopics(bootstrapKafkaUrl string, regex *regexp.Regexp) (result []string, err error) {
	broker, err := getBroker(bootstrapKafkaUrl)
	if err != nil {
		return result, err
	}
	client := &kafka.Client{
		Addr: kafka.TCP(broker...),
	}
	kafkaTopics, err := topics.ListRe(context.Background(), client, regex)
	if err != nil {
		return result, err
	}
	for _, topic := range kafkaTopics {
		if !topic.Internal {
			if topic.Error != nil {
				log.Println("WARNING: ", topic.Name, ":", topic.Error)
			} else {
				result = append(result, topic.Name)
			}
		}
	}
	sort.Strings(result)
	return result, nil
}
