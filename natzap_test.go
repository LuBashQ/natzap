package natzap

import (
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"testing"
)

func TestConnection(t *testing.T) {
	con, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Error(err)
	}

	con.Close()
}

func TestLogging(t *testing.T) {
	con, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Error(err)
	}

	encoder := zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig())
	core, err := NewCore(zapcore.WarnLevel, encoder, con).WithSubject("log").WithJetStream("LOG")
	if err != nil {
		t.Fatal(err)
	}
	logger := zap.New(core, zap.Development())

	logger.Warn("This is a test")

	con.Close()
}
