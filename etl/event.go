package etl

import (
	"sync"
)

type Event struct {
	Name    string
	Payload interface{}
}

type EventHandler func(Event)

type EventBus struct {
	handlers map[string][]EventHandler
	mu       sync.RWMutex
}

func NewEventBus() *EventBus {
	return &EventBus{
		handlers: make(map[string][]EventHandler),
	}
}

func (eb *EventBus) Subscribe(eventName string, handler EventHandler) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	eb.handlers[eventName] = append(eb.handlers[eventName], handler)
}

func (eb *EventBus) Publish(eventName string, payload interface{}) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	if hs, ok := eb.handlers[eventName]; ok {
		for _, h := range hs {
			go h(Event{Name: eventName, Payload: payload})
		}
	}
}
