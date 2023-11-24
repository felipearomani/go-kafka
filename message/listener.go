package message

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type (
	messageHandler interface {
		Handle(ctx context.Context, msg *kafka.Message) error
	}

	kafkaConsumer interface {
		SubscribeTopics(topics []string, rebalanceCB kafka.RebalanceCb) (err error)
		ReadMessage(timeout time.Duration) (*kafka.Message, error)
		Close() (err error)
	}

	ListenerConfig struct {
		Topic          string
		Consumer       kafkaConsumer
		Handler        messageHandler
		HandlerTimeout time.Duration
	}
)

type EventListener struct {
	config *ListenerConfig
	cancel chan struct{}
}

func NewEventListener(cfg *ListenerConfig) *EventListener {
	return &EventListener{
		config: cfg,
		cancel: make(chan struct{}),
	}
}

func (e *EventListener) Listen() error {
	var (
		consumer = e.config.Consumer
		topic    = e.config.Topic
	)

	err := consumer.SubscribeTopics([]string{topic}, func(_ *kafka.Consumer, event kafka.Event) error {
		slog.Info("rebalacing partitions", slog.String("topic", topic), slog.String("kafka_info", event.String()))
		return nil
	})
	if err != nil {
		return fmt.Errorf("erro on subscribe topic [%v]: %w", topic, err)
	}

loopMain:
	for {
		select {
		case <-e.cancel:
			break loopMain
		default:
			if err := e.handleMessage(); err != nil {
				slog.Error("error on handle kafka message", slog.String("error", err.Error()))
			}
		}
	}

	return nil
}

func (e *EventListener) handleMessage() error {
	var (
		consumer = e.config.Consumer
		timeout  = e.config.HandlerTimeout
		handler  = e.config.Handler
	)

	msg, err := consumer.ReadMessage(time.Second)
	if err != nil {
		if err.(kafka.Error).IsTimeout() {
			return nil
		}
		return fmt.Errorf("error on read message from kafka: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	if err := handler.Handle(ctx, msg); err != nil {
		return fmt.Errorf("error on handle kafka message: %w", err)
	}

	return nil
}

func (e *EventListener) Close() error {
	e.cancel <- struct{}{}
	if err := e.config.Consumer.Close(); err != nil {
		return err
	}

	return nil
}
