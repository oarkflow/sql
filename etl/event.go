package etl

import (
	"fmt"
	"log"
	"sync"
)

type Event struct {
	Name    string
	Payload any
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

func (eb *EventBus) Publish(eventName string, payload any) {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	if hs, ok := eb.handlers[eventName]; ok {
		for _, h := range hs {
			go func(handler EventHandler) {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("Recovered in event handler for event %s: %v", eventName, r)
					}
				}()
				handler(Event{Name: eventName, Payload: payload})
			}(h)
		}
	}
}

func (eb *EventBus) Unsubscribe(eventName string, handler EventHandler) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	handlers, ok := eb.handlers[eventName]
	if !ok {
		return
	}
	for i, h := range handlers {
		// Compare pointers by formatting addresses as strings.
		if fmt.Sprintf("%p", h) == fmt.Sprintf("%p", handler) {
			eb.handlers[eventName] = append(handlers[:i], handlers[i+1:]...)
			break
		}
	}
}

// ListSubscribers returns a map of event names to the count of handlers subscribed.
func (eb *EventBus) ListSubscribers() map[string]int {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	subs := make(map[string]int)
	for event, handlers := range eb.handlers {
		subs[event] = len(handlers)
	}
	return subs
}
