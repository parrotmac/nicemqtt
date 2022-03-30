package nicemqtt

type SimulatedClient struct {
}

func (c *SimulatedClient) Subscribe(topic string, qos byte, callback Callback) error {
	return nil
}

func (c *SimulatedClient) PublishBytes(topic string, qos byte, payload []byte) error {
	return nil
}

var _ Client = &SimulatedClient{}
