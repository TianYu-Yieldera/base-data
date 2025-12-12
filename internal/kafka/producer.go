package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type Producer struct {
	producer sarama.AsyncProducer
	logger   *zap.Logger
	wg       sync.WaitGroup
	done     chan struct{}
}

type ProducerConfig struct {
	Brokers         []string
	BatchSize       int
	BatchTimeout    time.Duration
	MaxRetries      int
	RequiredAcks    sarama.RequiredAcks
	CompressionType sarama.CompressionCodec
}

func NewProducer(cfg ProducerConfig, logger *zap.Logger) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = cfg.RequiredAcks
	config.Producer.Retry.Max = cfg.MaxRetries
	config.Producer.Compression = cfg.CompressionType
	config.Producer.Flush.Messages = cfg.BatchSize
	config.Producer.Flush.Frequency = cfg.BatchTimeout
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewAsyncProducer(cfg.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	p := &Producer{
		producer: producer,
		logger:   logger,
		done:     make(chan struct{}),
	}

	p.wg.Add(2)
	go p.handleSuccesses()
	go p.handleErrors()

	return p, nil
}

func (p *Producer) handleSuccesses() {
	defer p.wg.Done()
	for {
		select {
		case msg := <-p.producer.Successes():
			if msg != nil {
				p.logger.Debug("message sent successfully",
					zap.String("topic", msg.Topic),
					zap.Int32("partition", msg.Partition),
					zap.Int64("offset", msg.Offset))
			}
		case <-p.done:
			return
		}
	}
}

func (p *Producer) handleErrors() {
	defer p.wg.Done()
	for {
		select {
		case err := <-p.producer.Errors():
			if err != nil {
				p.logger.Error("failed to send message",
					zap.String("topic", err.Msg.Topic),
					zap.Error(err.Err))
			}
		case <-p.done:
			return
		}
	}
}

func (p *Producer) Send(ctx context.Context, topic string, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(data),
	}

	select {
	case p.producer.Input() <- msg:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (p *Producer) SendBatch(ctx context.Context, topic string, messages []Message) error {
	for _, m := range messages {
		if err := p.Send(ctx, topic, m.Key, m.Value); err != nil {
			return err
		}
	}
	return nil
}

func (p *Producer) Close() error {
	close(p.done)
	p.wg.Wait()
	return p.producer.Close()
}

type Message struct {
	Key   string
	Value interface{}
}
