package nicemqtt

const (
	QoSMaybeOnce   = 0
	QoSAtLeastOnce = 1
	QoSExactlyOnce = 2

	DefaultHost = "localhost"
	DefaultPort = 1883

	DefaultKeepAliveSeconds = 60

	SchemeMQTTS = "mqtts"
	SchemeMQTT  = "mqtt"
	SchemeTCP   = "tcp"
	SchemeTCPS  = "tcps"
	SchemeWS    = "ws"
	SchemeWSS   = "wss"

	DefaultScheme = SchemeMQTT
)
