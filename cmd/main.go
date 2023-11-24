package main

import (
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/felipearomani/go-kafka/kafkahandler"
	"github.com/felipearomani/go-kafka/message"
)

func main() {
	h := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: new(slog.LevelVar)})
	slog.SetDefault(slog.New(h))

	cfg := &kafka.ConfigMap{
		"bootstrap.servers": "localhost",
		"group.id":          "romani-test",
	}

	consumer, err := kafka.NewConsumer(cfg)
	if err != nil {
		slog.Error("err on create kafka consumer", slog.String("err", err.Error()))
		os.Exit(1)
	}

	handler := &kafkahandler.MyHandler{}
	l := message.NewEventListener("orders", consumer, handler, 5*time.Second)
	if err := l.Listen(); err != nil {
		log.Fatal(err)
	}
}
