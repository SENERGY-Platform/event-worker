/*
 * Copyright 2021 InfAI (CC SES)
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

package docker

import (
	"context"
	"errors"
	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/segmentio/kafka-go"
	"github.com/wvanbergen/kazoo-go"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

func Kafka(pool *dockertest.Pool, ctx context.Context, wg *sync.WaitGroup, zookeeperUrl string) (kafkaUrl string, err error) {
	kafkaport, err := GetFreePort()
	if err != nil {
		return "", errors.New("Could not find new port")
	}
	networks, _ := pool.Client.ListNetworks()
	hostIp := ""
	for _, network := range networks {
		if network.Name == "bridge" {
			hostIp = network.IPAM.Config[0].Gateway
		}
	}
	kafkaUrl = hostIp + ":" + strconv.Itoa(kafkaport)
	log.Println("host ip: ", hostIp)
	log.Println("kafka url:", kafkaUrl)
	env := []string{
		"KAFKA_DEFAULT_REPLICATION_FACTOR=1",
		"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"ALLOW_PLAINTEXT_LISTENER=yes",
		"KAFKA_LISTENERS=OUTSIDE://:9092",
		"KAFKA_ADVERTISED_LISTENERS=OUTSIDE://" + kafkaUrl,
		"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=OUTSIDE:PLAINTEXT",
		"KAFKA_INTER_BROKER_LISTENER_NAME=OUTSIDE",
		"KAFKA_ZOOKEEPER_CONNECT=" + zookeeperUrl,
	}
	log.Println("start kafka with env ", env)
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "bitnami/kafka",
		Tag:        "latest",
		Env:        env,
		PortBindings: map[docker.Port][]docker.PortBinding{
			"9092/tcp": {{HostIP: "", HostPort: strconv.Itoa(kafkaport)}},
		},
	}, func(config *docker.HostConfig) {
		config.Tmpfs = map[string]string{
			"/var/lib/kafka/data": "rw",
		}
	})
	if err != nil {
		return kafkaUrl, err
	}
	wg.Add(1)
	go func() {
		<-ctx.Done()
		log.Println("DEBUG: remove container " + container.Container.Name)
		container.Close()
		wg.Done()
	}()
	err = pool.Retry(func() error {
		log.Println("try kafka connection...")
		conn, err := kafka.Dial("tcp", hostIp+":"+strconv.Itoa(kafkaport))
		if err != nil {
			log.Println(err)
			return err
		}
		defer conn.Close()
		return nil
	})
	//go Dockerlog(pool, ctx, container, "KAFKA")
	time.Sleep(5 * time.Second)
	return kafkaUrl, err
}

func Zookeeper(pool *dockertest.Pool, ctx context.Context, wg *sync.WaitGroup) (hostPort string, ipAddress string, err error) {
	zkport, err := GetFreePort()
	if err != nil {
		return "", "", errors.New("Could not find new port")
	}
	env := []string{}
	log.Println("start zookeeper on ", zkport)
	container, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "wurstmeister/zookeeper",
		Tag:        "latest",
		Env:        env,
		PortBindings: map[docker.Port][]docker.PortBinding{
			"2181/tcp": {{HostIP: "", HostPort: strconv.Itoa(zkport)}},
		},
	}, func(config *docker.HostConfig) {
		config.Tmpfs = map[string]string{
			"/var/lib/zookeeper/data": "rw",
			"/var/lib/zookeeper/log":  "rw",
		}
	})
	if err != nil {
		return "", "", err
	}
	hostPort = strconv.Itoa(zkport)
	wg.Add(1)
	go func() {
		<-ctx.Done()
		log.Println("DEBUG: remove container " + container.Container.Name)
		container.Close()
		wg.Done()
	}()
	err = pool.Retry(func() error {
		log.Println("try zk connection...")
		zookeeper := kazoo.NewConfig()
		zk, chroot := kazoo.ParseConnectionString(container.Container.NetworkSettings.IPAddress)
		zookeeper.Chroot = chroot
		kz, err := kazoo.NewKazoo(zk, zookeeper)
		if err != nil {
			log.Println("kazoo", err)
			return err
		}
		_, err = kz.Brokers()
		if err != nil && strings.TrimSpace(err.Error()) != strings.TrimSpace("zk: node does not exist") {
			log.Println("brokers", err)
			return err
		}
		return nil
	})
	return hostPort, container.Container.NetworkSettings.IPAddress, err
}
