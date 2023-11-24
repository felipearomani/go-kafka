package message

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/google/go-cmp/cmp"
)

type stubMessageHandler struct {
	handle func(ctx context.Context, msg *kafka.Message) error
}

func (s *stubMessageHandler) Handle(ctx context.Context, msg *kafka.Message) error {
	return s.handle(ctx, msg)
}

type stubKafkaConsumer struct {
	subscribeTopics func(topics []string, rebalanceCB kafka.RebalanceCb) (err error)
	readMessage     func(timeout time.Duration) (*kafka.Message, error)
	close           func() error
}

func (k *stubKafkaConsumer) SubscribeTopics(topics []string, rebalanceCB kafka.RebalanceCb) (err error) {
	return k.subscribeTopics(topics, rebalanceCB)
}

func (k *stubKafkaConsumer) ReadMessage(timeout time.Duration) (*kafka.Message, error) {
	return k.readMessage(timeout)
}

func (k *stubKafkaConsumer) Close() error {
	return k.close()
}

type stubKafkaEvent struct {
}

func (e *stubKafkaEvent) String() string {
	return "mock kafka event"
}

func TestEventListener_Listen(t *testing.T) {
	wantMessage := &kafka.Message{Value: []byte("test message")}

	kc := &stubKafkaConsumer{
		readMessage: func(timeout time.Duration) (*kafka.Message, error) {
			<-time.After(timeout)
			return wantMessage, nil
		},
		subscribeTopics: func(topics []string, rebalanceCB kafka.RebalanceCb) (err error) {
			_ = rebalanceCB(nil, &stubKafkaEvent{})
			return nil
		},
		close: func() error {
			return nil
		},
	}

	var gotMessage *kafka.Message
	handler := &stubMessageHandler{
		handle: func(ctx context.Context, msg *kafka.Message) error {
			gotMessage = msg
			return nil

		},
	}

	el := NewEventListener("orders", kc, handler, 5*time.Second)

	go func() {
		<-time.After(time.Second)
		_ = el.Close()
	}()

	if err := el.Listen(); err != nil {
		t.Fatalf("got error [%v], want nil", err)
	}

	if diff := cmp.Diff(gotMessage, wantMessage); diff != "" {
		t.Fatalf("(-got,+want): %v\n", diff)
	}
}

func TestEventListener_Listen_Error(t *testing.T) {
	var (
		ErrSubscribeMock = errors.New("subscribe error")
	)

	testCases := map[string]struct {
		mockKafkaConsumer  *stubKafkaConsumer
		mockMessageHandler *stubMessageHandler
		wantErr            error
	}{
		"error on subscribe topic": {
			mockKafkaConsumer: &stubKafkaConsumer{
				subscribeTopics: func(topics []string, rebalanceCB kafka.RebalanceCb) (err error) {
					return ErrSubscribeMock
				},
				close: func() error {
					return nil
				},
			},
			mockMessageHandler: &stubMessageHandler{
				handle: func(ctx context.Context, msg *kafka.Message) error {
					return nil
				},
			},
			wantErr: ErrSubscribeMock,
		},
	}

	for title, tc := range testCases {
		t.Run(title, func(t *testing.T) {
			el := NewEventListener("orders", tc.mockKafkaConsumer, tc.mockMessageHandler, 5*time.Second)

			go func() {
				<-time.After(time.Second)
				_ = el.Close()
			}()

			gotErr := el.Listen()
			if !errors.Is(gotErr, tc.wantErr) {
				t.Fatalf("got error [%v], want error [%v]", gotErr, tc.wantErr)
			}
		})
	}

}
