/*
 * Copyright 2019 InfAI (CC SES)
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

package metrics

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"runtime/debug"
)

type Metrics struct {
	ConsumedMessages            prometheus.Counter
	EventsChecked               prometheus.Counter
	MessagesSkipped             prometheus.Counter
	EventsTriggered             prometheus.Counter
	MessageAges                 prometheus.Histogram
	EventRepoLockTime           prometheus.Counter
	DoDuration                  prometheus.Counter
	DeploymentUpdateSignalCount prometheus.Counter
	httphandler                 http.Handler
}

func New() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		ConsumedMessages: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "event_worker_messages_consumed",
			Help: "count of consumed messages received since startup",
		}),
		MessagesSkipped: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "event_worker_messages_skipped",
			Help: "count of skipped messages (to old) since startup",
		}),
		EventsChecked: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "event_worker_events_checked",
			Help: "count of checked events since startup",
		}),
		EventsTriggered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "event_worker_events_triggered",
			Help: "count of checked events since startup",
		}),
		MessageAges: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "event_worker_message_ages_s",
			Help:    "age of received messages in s",
			Buckets: []float64{1, 2.5, 5, 10, 20, 30, 50, 100, 200, 500, 1000},
		}),
		EventRepoLockTime: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "event_worker_event_repo_locke_time_ms",
			Help: "lock time of event repo handling in ms",
		}),
		DoDuration: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "event_worker_do_locke_time_ms",
			Help: "lock time of do handling in ms",
		}),
		DeploymentUpdateSignalCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "event_worker_deployment_update_signal_count",
			Help: "count of cache resets triggered by kafka signals",
		}),
		httphandler: promhttp.HandlerFor(
			reg,
			promhttp.HandlerOpts{
				Registry: reg,
			},
		),
	}

	reg.MustRegister(m.ConsumedMessages)
	reg.MustRegister(m.MessagesSkipped)
	reg.MustRegister(m.EventsChecked)
	reg.MustRegister(m.EventsTriggered)
	reg.MustRegister(m.MessageAges)
	reg.MustRegister(m.EventRepoLockTime)
	reg.MustRegister(m.DoDuration)
	reg.MustRegister(m.DeploymentUpdateSignalCount)

	return m
}

func (this *Metrics) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	log.Printf("%v [%v] %v \n", request.RemoteAddr, request.Method, request.URL)
	this.httphandler.ServeHTTP(writer, request)
}

func (this *Metrics) Serve(ctx context.Context, port string) *Metrics {
	if port == "" || port == "-" {
		return this
	}
	router := http.NewServeMux()

	router.Handle("/metrics", this)

	server := &http.Server{Addr: ":" + port, Handler: router}
	go func() {
		log.Println("listening on ", server.Addr, "for /metrics")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			debug.PrintStack()
			log.Fatal("FATAL:", err)
		}
	}()
	go func() {
		<-ctx.Done()
		log.Println("metrics shutdown", server.Shutdown(context.Background()))
	}()
	return this
}
