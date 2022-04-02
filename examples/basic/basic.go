package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/parrotmac/nicemqtt"
)

func main() {
	randoClientID := uuid.New().String()
	client, err := nicemqtt.NewClient(os.Getenv("MQTT_HOST"), 1883, randoClientID)
	if err != nil {
		log.Fatalln("Could not connect client", err)
	}

	connCtx, cancelConn := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancelConn()

	if err := client.Connect(connCtx); err != nil {
		log.Fatalln("Could not connect client", err)
	}

	log.Println("Connected")

	client.Subscribe("#", nicemqtt.QoSAtLeastOnce, func(topic string, payload []byte) {
		log.Printf("Received message on topic %s: %s", topic, string(payload))
	})

	for {
		client.Publish(context.TODO(), "foo/bar", nicemqtt.QoSAtLeastOnce, false, []byte("ok:1"))
		time.Sleep(time.Second * 10)
	}
}
