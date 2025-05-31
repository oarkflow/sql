package mqadapter

import (
	"context"

	"github.com/oarkflow/json"
	"github.com/oarkflow/log"
	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/oarkflow/sql/pkg/config"
	"github.com/oarkflow/sql/pkg/contracts"
	"github.com/oarkflow/sql/pkg/utils"
)

type Adapter struct {
	config    config.DataConfig
	conn      *amqp.Connection
	channel   *amqp.Channel
	queueName string
	consumer  <-chan amqp.Delivery
}

func New(cfg config.DataConfig) contracts.LookupLoader {
	return &Adapter{config: cfg}
}

func (a *Adapter) Setup(ctx context.Context) error {
	// use config.Source as the AMQP connection URI
	conn, err := amqp.Dial(a.config.Source)
	if err != nil {
		return err
	}
	a.conn = conn
	ch, err := conn.Channel()
	if err != nil {
		return err
	}
	a.channel = ch
	// use config.Table as queue name (if empty, use "default")
	a.queueName = a.config.Table
	if a.queueName == "" {
		a.queueName = "default"
	}
	_, err = ch.QueueDeclare(
		a.queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	return nil
}

func (a *Adapter) StoreBatch(ctx context.Context, records []utils.Record) error {
	for _, rec := range records {
		data, err := json.Marshal(rec)
		if err != nil {
			return err
		}
		err = a.channel.Publish(
			"", // default exchange
			a.queueName,
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        data,
			},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func (a *Adapter) LoadData(opts ...contracts.Option) ([]utils.Record, error) {
	ch, err := a.Extract(context.Background(), opts...)
	if err != nil {
		return nil, err
	}
	var records []utils.Record
	for rec := range ch {
		records = append(records, rec)
	}
	return records, nil
}

func (a *Adapter) Extract(ctx context.Context, opts ...contracts.Option) (<-chan utils.Record, error) {
	if a.consumer == nil {
		consumer, err := a.channel.Consume(
			a.queueName,
			"",
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return nil, err
		}
		a.consumer = consumer
	}
	out := make(chan utils.Record, 100)
	go func() {
		defer close(out)
		for d := range a.consumer {
			var rec utils.Record
			if err := json.Unmarshal(d.Body, &rec); err != nil {
				log.Printf("MQ unmarshal error: %v", err)
				continue
			}
			out <- rec
		}
	}()
	return out, nil
}

func (a *Adapter) Close() error {
	if a.channel != nil {
		_ = a.channel.Close()
	}
	if a.conn != nil {
		return a.conn.Close()
	}
	return nil
}
