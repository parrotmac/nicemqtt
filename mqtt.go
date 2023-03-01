package nicemqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net/url"
	"time"

	"github.com/cenkalti/backoff"

	paho "github.com/eclipse/paho.mqtt.golang"
)

type Message struct {
	QoS     byte
	Retain  bool
	Topic   string
	Payload []byte
}

type Callback func(topic string, payload []byte)

type Client interface {
	Subscribe(topic string, qos byte, callback Callback) error
	Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error
	Connect(ctx context.Context) error
}

type subscription struct {
	topicPattern string
	qos          byte
}

type DefaultClient struct {
	host           string
	port           int
	keepalive      int64
	useTLS         bool
	tlsPrivateKey  string
	tlsCertificate string
	scheme         string
	clientID       string

	additionalRootCAs    []string
	discardSystemRootCAs bool

	subscriptions map[subscription]Callback

	pahoClient paho.Client
}

func NewClient(host string, port int, clientID string) (Client, error) {
	return NewClientWithOptions(host, port, clientID)
}

type ClientOption (func(*DefaultClient))

func WithKeepalive(keepalive int64) ClientOption {
	return func(c *DefaultClient) {
		c.keepalive = keepalive
	}
}

func WithScheme(scheme string) ClientOption {
	return func(c *DefaultClient) {
		c.scheme = scheme
	}
}

func WithTLS(tlsPrivateKey, tlsCertificate string) ClientOption {
	return func(c *DefaultClient) {
		c.useTLS = true
		c.tlsPrivateKey = tlsPrivateKey
		c.tlsCertificate = tlsCertificate
		c.scheme = SchemeMQTTS
	}
}

func WithExclusiveRootCAs(caCertificates []string) ClientOption {
	return func(c *DefaultClient) {
	}
}

func WithAdditionalRootCAs(caCertificates []string) ClientOption {
	return func(c *DefaultClient) {
	}
}

func (c *DefaultClient) initPahoClient() error {
	pahoOpts := paho.NewClientOptions()

	pahoOpts.AddBroker(fmt.Sprintf("%s://%s:%d", c.scheme, c.host, c.port))
	pahoOpts.AutoReconnect = true
	pahoOpts.SetClientID(c.clientID)
	pahoOpts.ConnectRetry = true
	pahoOpts.ConnectRetryInterval = time.Second * 5
	pahoOpts.CleanSession = true
	pahoOpts.KeepAlive = c.keepalive
	pahoOpts.MaxReconnectInterval = time.Second * 30
	pahoOpts.ResumeSubs = true
	pahoOpts.OnReconnecting = c.pahoOnReconnecting
	pahoOpts.OnConnect = c.pahoOnConnect
	pahoOpts.OnConnectionLost = c.pahoOnConnectionLost
	pahoOpts.OnConnectAttempt = c.pahoOnConnectAttempt

	if c.useTLS {

		rootCAs := x509.NewCertPool()
		if !c.discardSystemRootCAs {
			var err error
			rootCAs, err = x509.SystemCertPool()
			if err != nil {
				return fmt.Errorf("Could not load system root CAs: %s", err)
			}
		}
		for _, caCertificate := range c.additionalRootCAs {
			rootCAs.AppendCertsFromPEM([]byte(caCertificate))
		}

		pahoOpts.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{
				{
					Certificate: [][]byte{[]byte(c.tlsCertificate)},
					PrivateKey:  []byte(c.tlsPrivateKey),
				},
			},
			RootCAs: rootCAs,
		}
	}

	c.pahoClient = paho.NewClient(pahoOpts)

	return nil
}

func NewClientWithOptions(host string, port int, clientID string, options ...ClientOption) (Client, error) {
	client := &DefaultClient{
		host:           host,
		port:           port,
		keepalive:      DefaultKeepAliveSeconds,
		useTLS:         false,
		tlsPrivateKey:  "",
		tlsCertificate: "",
		scheme:         DefaultScheme,
		clientID:       clientID,

		subscriptions: make(map[subscription]Callback),

		// Filled below
		pahoClient: nil,
	}

	for _, option := range options {
		option(client)
	}

	if err := client.initPahoClient(); err != nil {
		return nil, fmt.Errorf("Could not initialize paho client: %s", err)
	}

	return client, nil
}

func (c *DefaultClient) Connect(ctx context.Context) error {
	if c.pahoClient == nil {
		return fmt.Errorf("Client not initialized")
	}

	b := backoff.WithContext(backoff.NewExponentialBackOff(), ctx)

	token := c.pahoClient.Connect()

	connectOperation := func() error {
		log.Println("Attempting connection... next retry in", b.NextBackOff())

		scopedCtx, cancel := context.WithTimeout(b.Context(), b.NextBackOff())
		defer cancel()

		deadline, ok := scopedCtx.Deadline()
		if !ok {
			log.Println("No deadline; waiting on underlying transport for connection")
			didConnect := token.Wait()
			if !didConnect {
				return fmt.Errorf("Could not connect to broker")
			}
			return token.Error()
		}
		log.Printf("Waiting up to %s for connection", deadline.Sub(time.Now()))

		if !token.WaitTimeout(deadline.Sub(time.Now())) {
			log.Println("No connection within deadline", token.Error())
			// token.Error() will _not_ return an error if we reached the timeout/deadline
			// Return an error to force the backoff algorithm to retry
			return errors.New("failed to connect within deadline")
		}
		return token.Error()
	}

	err := backoff.Retry(connectOperation, b)
	if err != nil {
		return err
	}

	if c.pahoClient.IsConnected() {
		log.Println("Connected")
	} else {
		log.Panicln("Could not connect to broker")
	}

	return nil
}

func (c *DefaultClient) Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error {
	token := c.pahoClient.Publish(topic, qos, retain, payload)

	deadline, ok := ctx.Deadline()
	if !ok {
		token.Wait()
		return token.Error()
	}
	dur := time.Now().Sub(deadline)
	if token.WaitTimeout(dur) {
		return nil
	}
	return token.Error()
}

func (c *DefaultClient) attachSubscription(topic string, qos byte, callback Callback) error {
	if c.pahoClient == nil {
		return fmt.Errorf("client not initialized")
	}

	if token := c.pahoClient.Subscribe(topic, qos, func(client paho.Client, msg paho.Message) {
		callback(msg.Topic(), msg.Payload())
	}); token.Error() != nil {
		return fmt.Errorf("could not subscribe: %s", token.Error())
	}

	return nil
}

func (c *DefaultClient) pahoOnReconnecting(client paho.Client, o *paho.ClientOptions) {
	log.Println("Reconnecting...")
}

func (c *DefaultClient) pahoOnConnect(client paho.Client) {
	// It seems that subscriptions aren't retained upon reconnection so we need to re-subscribe
	// after reconnection. If we can leverage a reconnection at the Paho level we can remove this!
	for sub, callback := range c.subscriptions {
		if err := c.attachSubscription(sub.topicPattern, sub.qos, callback); err != nil {
			log.Printf("Could not attach subscription: %s", err)
		}
	}
	log.Println("Connected")
}

func (c *DefaultClient) pahoOnConnectionLost(client paho.Client, err error) {
	log.Println("Connection lost", err)
}

func (c *DefaultClient) pahoOnConnectAttempt(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
	return tlsCfg
}

func (c *DefaultClient) Subscribe(topic string, qos byte, callback Callback) error {
	if c.pahoClient == nil {
		return fmt.Errorf("Client not initialized")
	}

	c.subscriptions[subscription{topic, qos}] = callback
	c.attachSubscription(topic, qos, callback)
	return nil
}
