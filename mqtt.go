package nicemqtt

import (
	"fmt"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

type SimpleClient struct {
	client mqtt.Client
}

type Callback mqtt.MessageHandler

func PlaintextClient(clientID string, serverHostname, serverPort string) (*SimpleClient, error) {
	brokerURI := fmt.Sprintf("tcp://%s:%s", serverHostname, serverPort)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(brokerURI)
	opts.SetClientID(clientID)

	client := mqtt.NewClient(opts)
	token := client.Connect()

	log.Println("Connecting to broker", brokerURI)
	var attempts int64
	for {
		log.Printf("Connection attempt %d:\n", attempts+1)
		if token.WaitTimeout(time.Second * 5) {
			log.Println("Connection Success!")
			break
		}
		log.Println("Connection Failed. Waiting a moment before retrying...")
		time.Sleep(time.Second * 5)
		attempts++

		if attempts > 100 {
			log.Fatalln("Connection failed after 100 attempts; killing application.")
		}
	}

	return &SimpleClient{
		client: client,
	}, token.Error()
}

func (c *SimpleClient) Subscribe(topic string, qos byte, callback Callback) error {
	// TODO: Use our own callback type
	token := c.client.Subscribe(topic, qos, mqtt.MessageHandler(callback))
	if err := token.Error(); err != nil {
		return err
	}
	token.Wait()
	return nil
}

func (c *SimpleClient) PublishBytes(topic string, qos byte, payload []byte) error {
	token := c.client.Publish(topic, qos, false, payload)
	if err := token.Error(); err != nil {
		return err
	}
	token.Wait()
	return nil
}

type Client interface {
	Subscribe(topic string, qos byte, callback Callback) error
	PublishBytes(topic string, qos byte, payload []byte) error
}
