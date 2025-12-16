package cloud

import (
	"context"
	"sync"

	"github.com/SENERGY-Platform/event-worker/pkg/configuration"
	"github.com/SENERGY-Platform/event-worker/pkg/model"
)

type UpdatableConsumer struct {
	config      configuration.Config
	onMsg       func(msg model.ConsumerMessage) error
	onError     func(topic string, err error)
	topics      []string
	lock        sync.Mutex
	wg          *sync.WaitGroup
	basectx     context.Context
	consumerctx context.Context
	close       context.CancelFunc
}

func NewUpdatableConsumer(ctx context.Context, config configuration.Config, onMsg func(msg model.ConsumerMessage) error, onError func(topic string, err error)) *UpdatableConsumer {
	if onMsg == nil {
		onMsg = func(msg model.ConsumerMessage) error { return nil }
	}
	if onError == nil {
		onError = func(topic string, err error) {}
	}
	return &UpdatableConsumer{basectx: ctx, config: config, onMsg: onMsg, onError: onError}
}

func (this *UpdatableConsumer) UpdateTopics(topics []string) error {
	if listChanged(this.topics, topics) {
		this.config.GetLogger().Info("update topics of consumer", "topics-count", len(topics))
		this.topics = topics
		this.lock.Lock()
		defer this.lock.Unlock()
		if this.close != nil {
			this.close()
		}
		if this.wg != nil {
			this.wg.Wait()
		}
		this.consumerctx, this.close = context.WithCancel(this.basectx)
		this.wg = &sync.WaitGroup{}
		err := NewKafkaLastOffsetConsumerGroup(this.consumerctx, this.wg, this.config.KafkaUrl, this.config.KafkaConsumerGroup, this.topics, this.onMsg, this.onError)
		if err != nil {
			this.config.GetLogger().Error("failed to start new consumer with updated topics", "error", err)
			return err
		}
	}
	return nil
}
