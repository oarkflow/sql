package events

import (
	"context"
	"sync"
	"time"
)

type EventType string

const (
	EventFileArrival    EventType = "file_arrival"
	EventWebhook        EventType = "webhook"
	EventDatasetUpdated EventType = "dataset_updated"
	EventJobCompleted   EventType = "job_completed"
	EventJobFailed      EventType = "job_failed"
)

type Event struct {
	Type      EventType
	Source    string
	Payload   map[string]any
	Timestamp time.Time
}

type Handler func(ctx context.Context, event Event) error

type EventBus struct {
	handlers map[EventType][]Handler
	mu       sync.RWMutex
}

func NewEventBus() *EventBus {
	return &EventBus{
		handlers: make(map[EventType][]Handler),
	}
}

func (b *EventBus) Subscribe(eventType EventType, handler Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers[eventType] = append(b.handlers[eventType], handler)
}

func (b *EventBus) Publish(ctx context.Context, event Event) error {
	b.mu.RLock()
	handlers, ok := b.handlers[event.Type]
	b.mu.RUnlock()

	if !ok {
		return nil
	}

	// In a real distributed system, this might push to a queue (NATS/Kafka)
	// Here we execute handlers (potentially async)
	for _, h := range handlers {
		go func(handler Handler) {
			_ = handler(ctx, event)
		}(h)
	}
	return nil
}

// Trigger represents a configured trigger for a pipeline
type Trigger struct {
	Type      EventType
	Condition func(Event) bool
	Action    func() error
}
