package nicemqtt

import (
	"context"
	"strings"
	"sync"
	"testing"
)

func TestSimulatedClient(t *testing.T) {
	cx := NewSimulatedClient()

	err := cx.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	responses := make(chan *Message, 10)

	cx.Subscribe("foo/+", QoSAtLeastOnce, func(topic string, payload []byte) {
		t.Log("Received message on topic", topic)
		responses <- &Message{
			Topic:   topic,
			Payload: payload,
		}
	})

	t.Log("Finished subscribing")

	topicMessageSets := []struct {
		topic   string
		payload []byte
	}{
		{"foo/bar", []byte("ok:1")},
		{"foo/bar", []byte("ok:2")},
		{"foo", []byte("drop:3")},
		{"foo/bar/abc", []byte("drop:4")},
		{"foo/foo", []byte("ok:5")},
	}

	wg := &sync.WaitGroup{}
	for _, tm := range topicMessageSets {
		wg.Add(1)
		go func(topic string, payload []byte) {
			cx.Publish(context.TODO(), topic, QoSAtLeastOnce, false, payload)
			wg.Done()
		}(tm.topic, tm.payload)
	}
	wg.Wait()

	t.Log("Finished publishing")

	numReceived := 0

	for i := 0; i < 3; i++ {
		msg := <-responses

		t.Log("Received message:", msg)
		numReceived++
		if len(strings.Split(msg.Topic, "/")) == 2 && strings.HasPrefix(string(msg.Payload), "ok") {
			continue
		}
		t.Errorf("Received unexpected message: %+v", msg)
	}

	if numReceived != 3 {
		t.Errorf("Expected 3 messages, received %d", numReceived)
	}

	select {
	case msg := <-responses:
		t.Error("Received unexpected message:", msg)
	default:
		break
	}
}

func TestMatchTopic(t *testing.T) {
	type args struct {
		topic string
		match string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{
			name: "match",
			args: args{
				topic: "foo/bar",
				match: "foo/#",
			},
			want: true,
		},
		{
			name: "match",
			args: args{
				topic: "foo/bar/baz/cat",
				match: "foo/#",
			},
			want: true,
		},
		{
			name: "no match",
			args: args{
				topic: "foo/bar",
				match: "foo/baz",
			},
			want: false,
		},
		{
			name: "no match",
			args: args{
				topic: "foo/bar",
				match: "foo/bar/baz",
			},
			want: false,
		},
		{
			name: "no match",
			args: args{
				topic: "foo/bar",
				match: "foo/bar/baz/#",
			},
			want: false,
		},
		{
			name: "match",
			args: args{
				topic: "foo/bar/cat/baz",
				match: "foo/bar/+/baz",
			},
			want: true,
		},
		{
			name: "no match",
			args: args{
				topic: "foo/bar",
				match: "foo/bar/baz/#",
			},
			want: false,
		},
		{
			name: "no match",
			args: args{
				topic: "foo/bar",
				match: "foo/+/baz",
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchTopic(tt.args.topic, tt.args.match); got != tt.want {
				t.Errorf("matchTopic(%s, %s) = %v, want %v", tt.args.topic, tt.args.match, got, tt.want)
			}
		})
	}
}
