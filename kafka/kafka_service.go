package kafka

import (
	"blaster/core"
	"context"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
)

type KafkaService struct {
	messages chan *core.Message
}

func (b *KafkaService) Read() ([]*core.Message, error) {
	select {
	case m := <-b.messages:
		return []*core.Message{m}, nil
	case <-time.After(time.Second):
		return []*core.Message{}, nil
	}
}

func (b *KafkaService) Delete(m *core.Message) error {
	return nil
}

func (b *KafkaService) Poison(m *core.Message) error {
	return nil
}

func (b *KafkaService) Messages() chan<- *core.Message {
	return b.messages
}

func NewKafkaService(bufferSize int) *KafkaService {
	return &KafkaService{
		messages: make(chan *core.Message, bufferSize),
	}
}

type PartionHandler struct {
	KafkaService   *KafkaService
	MessagePump    *core.MessagePump
	HandlerManager *core.HandlerManager
}

func (h *PartionHandler) Start(ctx context.Context) {
	h.HandlerManager.Start(ctx)
	h.MessagePump.Start(ctx)
}

type SaramaConsumerGroupHandler struct {
	PartionHandlers map[int32]map[int32]*PartionHandler
	Mutex           sync.Mutex
	Binding         *KafkaBinding
	Context         context.Context
}

func (h *SaramaConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.Mutex.Lock()
	defer h.Mutex.Unlock()

	config := h.Binding.Config
	table := make(map[int32]*PartionHandler)
	for _, partions := range session.Claims() {
		for _, p := range partions {
			dispatcher := core.NewHttpDispatcher(config.HandlerURL)
			qsvc := NewKafkaService(h.Binding.BufferSize)
			pump := core.NewMessagePump(qsvc, dispatcher, config.RetryCount, config.RetryDelay, config.MaxHandlers)
			hm := core.NewHandlerManager(config.HandlerCommand, config.HandlerArgs, config.HandlerURL, config.StartupDelaySeconds)
			ph := &PartionHandler{
				KafkaService:   qsvc,
				MessagePump:    pump,
				HandlerManager: hm,
			}

			ph.Start(session.Context())
			table[p] = ph
		}
	}
	h.PartionHandlers[session.GenerationID()] = table
	return nil
}

func (h *SaramaConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	// TODO: Cleanup the partion handlers
	return nil
}

func (h *SaramaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	h.Mutex.Lock()
	table, ok := h.PartionHandlers[session.GenerationID()]
	if !ok {
		return errors.WithStack(errors.New("unable to consume a claim with an unclaimed generation"))
	}
	p, ok := table[claim.Partition()]
	if !ok {
		return errors.WithStack(errors.New("unable to consume a claim with an unclaimed partion"))
	}
	h.Mutex.Unlock()

	// Loop until the messages channel is open or
	// one of partion handler components exit.
	// When a partion handler component exit, we return
	// from the method and leave Sarama to cancel the
	// session. Since session's context is associated
	// with both MessagePump and HandlerManager, this
	// should gracefully shutdown all components.
loop:
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				break loop
			}
			m := &core.Message{
				MessageID: string(msg.Key),
				Body:      string(msg.Value),
			}
			p.KafkaService.Messages() <- m
		case <-p.HandlerManager.Done:
			break loop
		case <-p.MessagePump.Done:
			break loop
		}
	}
	return nil
}

type KafkaBinding struct {
	Topic           string
	Group           sarama.ConsumerGroup
	BrokerAddresses []string
	BufferSize      int
	Config          *core.Config
	done            chan error
}

func (b *KafkaBinding) Start(ctx context.Context) {
	defer b.Group.Close()

	// Iterate over consumer sessions.
	for {
		topics := []string{b.Topic}
		handler := &SaramaConsumerGroupHandler{
			Binding:         b,
			Context:         ctx,
			PartionHandlers: make(map[int32]map[int32]*PartionHandler),
		}

		// `Consume` should be called inside an infinite loop, when a
		// server-side rebalance happens, the consumer session will need to be
		// recreated to get the new claims
		select {
		case <-ctx.Done():
			b.close(nil)
			return
		default:
			err := b.Group.Consume(ctx, topics, handler)
			if err != nil {
				b.close(err)
				return
			}
		}
	}
}

func (b *KafkaBinding) Done() <-chan error {
	return b.done
}

func (b *KafkaBinding) close(err error) {
	b.done <- err
	close(b.done)
}

func NewKafkaBinding(topic, group string, brokerAddresses []string, coreConfig *core.Config) (*KafkaBinding, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_4_0_0 // specify appropriate version
	config.Consumer.Return.Errors = true

	g, err := sarama.NewConsumerGroup(brokerAddresses, group, config)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Track errors
	go func() {
		for err := range g.Errors() {
			log.WithFields(log.Fields{"module": "kafka_binding", "err": err}).Info("error in consumer group")
		}
	}()

	return &KafkaBinding{
		Topic:      topic,
		Group:      g,
		Config:     coreConfig,
		BufferSize: 10,
		done:       make(chan error),
	}, nil
}
