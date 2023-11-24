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
)

type EventListener struct {
	topic          string
	consumer       kafkaConsumer
	handler        messageHandler
	handlerTimeOut time.Duration
	isRunning      bool
	cancel         chan struct{}
}

func NewEventListener(topic string, consumer kafkaConsumer, handler messageHandler, handlerTimeout time.Duration) *EventListener {
	return &EventListener{
		topic:          topic,
		consumer:       consumer,
		handler:        handler,
		handlerTimeOut: handlerTimeout,
		cancel:         make(chan struct{}),
	}
}

func (e *EventListener) Listen() error {
	err := e.consumer.SubscribeTopics([]string{e.topic}, func(_ *kafka.Consumer, event kafka.Event) error {
		slog.Info("rebalacing partitions", slog.String("topic", e.topic), slog.String("kafka_info", event.String()))
		return nil
	})
	if err != nil {
		return fmt.Errorf("erro on subscribe topic [%v]: %w", e.topic, err)
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
	msg, err := e.consumer.ReadMessage(time.Second)
	if err != nil {
		if err.(kafka.Error).IsTimeout() {
			return nil
		}
		return fmt.Errorf("error on read message from kafka: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.handlerTimeOut)
	defer cancel()

	if err := e.handler.Handle(ctx, msg); err != nil {
		return fmt.Errorf("error on handle kafka message: %w", err)
	}

	return nil
}

func (e *EventListener) Close() error {
	e.cancel <- struct{}{}
	if err := e.consumer.Close(); err != nil {
		return err
	}

	return nil
}
