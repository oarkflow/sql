package platform

import (
	"fmt"
	"log"
	"sync"
)

// InMemoryMessageBus implements MessageBus interface with in-memory channels
type InMemoryMessageBus struct {
	subscribers map[string][]chan any
	mu          sync.RWMutex
	running     bool
}

func NewInMemoryMessageBus() *InMemoryMessageBus {
	return &InMemoryMessageBus{
		subscribers: make(map[string][]chan any),
		running:     true,
	}
}

func (mb *InMemoryMessageBus) Publish(topic string, message any) error {
	mb.mu.RLock()
	defer mb.mu.RUnlock()

	if !mb.running {
		return fmt.Errorf("message bus is not running")
	}

	subscribers, exists := mb.subscribers[topic]
	if !exists {
		log.Printf("No subscribers for topic: %s", topic)
		return nil
	}

	// Send to all subscribers asynchronously
	for _, ch := range subscribers {
		go func(c chan any) {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("Recovered from panic in message bus subscriber: %v", r)
				}
			}()
			select {
			case c <- message:
				// Message sent
			default:
				log.Printf("Subscriber channel full, dropping message for topic: %s", topic)
			}
		}(ch)
	}

	return nil
}

func (mb *InMemoryMessageBus) Subscribe(topic string, handler func(message any)) error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	if !mb.running {
		return fmt.Errorf("message bus is not running")
	}

	ch := make(chan any, 100) // Buffered channel

	// Add to subscribers
	mb.subscribers[topic] = append(mb.subscribers[topic], ch)

	// Start goroutine to handle messages
	go func() {
		for mb.running {
			select {
			case msg := <-ch:
				handler(msg)
			}
		}
	}()

	log.Printf("Subscribed to topic: %s", topic)
	return nil
}

func (mb *InMemoryMessageBus) Close() error {
	mb.mu.Lock()
	defer mb.mu.Unlock()

	mb.running = false

	// Close all channels
	for topic, channels := range mb.subscribers {
		for _, ch := range channels {
			close(ch)
		}
		log.Printf("Closed %d subscribers for topic: %s", len(channels), topic)
	}

	// Clear subscribers
	mb.subscribers = make(map[string][]chan any)

	log.Printf("Message bus closed")
	return nil
}

// NATSMessageBus implements MessageBus using NATS
type NATSMessageBus struct {
	url         string
	connection  any // Would be *nats.Conn in real implementation
	subscribers map[string]any
	mu          sync.RWMutex
}

func NewNATSMessageBus(url string) *NATSMessageBus {
	return &NATSMessageBus{
		url:         url,
		subscribers: make(map[string]any),
	}
}

func (mb *NATSMessageBus) Publish(topic string, message any) error {
	// In real implementation, this would use NATS client
	log.Printf("NATS Publish to topic %s: %+v", topic, message)
	return nil
}

func (mb *NATSMessageBus) Subscribe(topic string, handler func(message any)) error {
	// In real implementation, this would subscribe to NATS topic
	log.Printf("NATS Subscribe to topic: %s", topic)
	return nil
}

func (mb *NATSMessageBus) Close() error {
	// In real implementation, this would close NATS connection
	log.Printf("NATS Message bus closed")
	return nil
}

// KafkaMessageBus implements MessageBus using Kafka
type KafkaMessageBus struct {
	brokers     []string
	producer    any // Would be sarama.SyncProducer in real implementation
	consumer    any // Would be sarama.ConsumerGroup in real implementation
	subscribers map[string]func(any)
	mu          sync.RWMutex
}

func NewKafkaMessageBus(brokers []string) *KafkaMessageBus {
	return &KafkaMessageBus{
		brokers:     brokers,
		subscribers: make(map[string]func(any)),
	}
}

func (mb *KafkaMessageBus) Publish(topic string, message any) error {
	// In real implementation, this would use Kafka producer
	log.Printf("Kafka Publish to topic %s: %+v", topic, message)
	return nil
}

func (mb *KafkaMessageBus) Subscribe(topic string, handler func(message any)) error {
	// In real implementation, this would use Kafka consumer
	log.Printf("Kafka Subscribe to topic: %s", topic)
	mb.subscribers[topic] = handler
	return nil
}

func (mb *KafkaMessageBus) Close() error {
	// In real implementation, this would close Kafka connections
	log.Printf("Kafka Message bus closed")
	return nil
}

// MessageBusFactory creates message bus instances
type MessageBusFactory struct{}

func NewMessageBusFactory() *MessageBusFactory {
	return &MessageBusFactory{}
}

func (f *MessageBusFactory) CreateMessageBus(config map[string]any) (MessageBus, error) {
	busType, ok := config["type"].(string)
	if !ok {
		busType = "in-memory"
	}

	switch busType {
	case "in-memory":
		return NewInMemoryMessageBus(), nil
	case "nats":
		url, _ := config["url"].(string)
		if url == "" {
			url = "nats://localhost:4222"
		}
		return NewNATSMessageBus(url), nil
	case "kafka":
		brokers, _ := config["brokers"].([]string)
		if len(brokers) == 0 {
			brokers = []string{"localhost:9092"}
		}
		return NewKafkaMessageBus(brokers), nil
	default:
		return nil, fmt.Errorf("unsupported message bus type: %s", busType)
	}
}
