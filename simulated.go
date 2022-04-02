package nicemqtt

import (
	"context"
	"errors"
	"math/rand"
	"strings"
	"time"
)

type SimulatedClient struct {
	subscriptions    map[subscription]Callback
	incomingMessages chan Message
	connected        bool

	// Percent chance of either dropping or over-delivering a message
	chaosFactor *float64
}

func NewSimulatedClient() *SimulatedClient {
	c := &SimulatedClient{
		subscriptions:    make(map[subscription]Callback),
		incomingMessages: make(chan Message, 10),
	}
	go c.routeMessages()
	return c
}

func NewLossySimulatedClient() *SimulatedClient {
	factor := 0.75 /* 75% chance of something going wrong */
	rand.Seed(time.Now().UnixNano())
	c := &SimulatedClient{
		subscriptions:    make(map[subscription]Callback),
		incomingMessages: make(chan Message, 10),
		chaosFactor:      &factor,
	}
	go c.routeMessages()
	return c
}

func matchTopic(topic string, pattern string) bool {
	// matchTopic implements MQTT-style topic matching.
	// Examples:
	//  "a/b/c" matches "a/#"
	//  "a/b" matches "a/+"
	//  "a/b/c" matches "a/+/c"
	//  "a/b/c" matches "a/b/c"
	//  "a/b/c" does not matche "a/+"
	//  "a/b/c" does not match "a/b/c/d"
	//  "a/b/c" does not match "a/b/d"
	//  "a/b/c" does not match "a/b"
	topicParts := strings.Split(topic, "/")
	patternParts := strings.Split(pattern, "/")

	for _, patternPart := range patternParts {
		if patternPart == "+" {
			if len(topicParts) == 0 {
				return false
			}
			topicParts = topicParts[1:]
			continue
		}
		if patternPart == "#" {
			return true
		}
		if len(topicParts) == 0 {
			return false
		}
		if topicParts[0] != patternPart {
			return false
		}
		topicParts = topicParts[1:]
	}
	return len(topicParts) == 0
}

func (c *SimulatedClient) chaos(fn func()) {
	if c.chaosFactor == nil {
		fn()
		return
	}

	if rand.Float64() >= *c.chaosFactor {
		fn()
		return
	}

	numDeliveries := rand.Intn(3)
	for i := 0; i < numDeliveries; i++ {
		fn()
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
	}
}

func (c *SimulatedClient) routeMessages() {
	for {
		select {
		case msg := <-c.incomingMessages:
			for subscr, callback := range c.subscriptions {
				if matchTopic(msg.Topic, subscr.topicPattern) {
					c.chaos(func() {
						callback(msg.Topic, msg.Payload)
					})
				}
			}
		}
	}
}

func (c *SimulatedClient) Connect(context.Context) error {
	c.connected = true
	return nil
}

func (c *SimulatedClient) Subscribe(topic string, qos byte, callback Callback) error {
	if !c.connected {
		return errors.New("not connected")
	}
	c.subscriptions[subscription{topic, qos}] = callback
	return nil
}

func (c *SimulatedClient) Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error {
	if !c.connected {
		return errors.New("not connected")
	}
	select {
	case c.incomingMessages <- Message{
		Topic:   topic,
		Payload: payload,
		QoS:     qos,
		Retain:  retain,
	}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

var _ Client = &SimulatedClient{}
