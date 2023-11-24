package kafkahandler

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type MyHandler struct {
}

func (m *MyHandler) Handle(ctx context.Context, msg *kafka.Message) error {
	fmt.Println("my message: ", string(msg.Value))
	return nil
}
