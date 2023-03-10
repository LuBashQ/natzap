package natzap

import (
	"errors"
	"github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"testing"
)

func newServer() *server.Server {
	opts := natstest.DefaultTestOptions
	opts.JetStream = true
	return natstest.RunServer(&opts)
}

func TestConnection(t *testing.T) {
	s := newServer()
	defer s.Shutdown()
	con, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Error(err)
	}

	con.Close()
}

func TestLoggingNoJetStream(t *testing.T) {
	s := newServer()
	defer s.Shutdown()
	con, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Error(err)
	}
	encoder := zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig())
	core := NewCore(zapcore.WarnLevel, encoder, con).WithSubject("log")
	logger := zap.New(core, zap.Development())
	logger.Warn("This is a test")
	con.Close()
}

func TestLoggingWithJetStream(t *testing.T) {
	s := newServer()
	defer s.Shutdown()
	con, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		t.Error(err)
	}
	encoder := zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig())
	core, err := NewCore(zapcore.WarnLevel, encoder, con).WithSubject("log").WithJetStream("LOG")
	if errors.Is(err, nats.ErrStreamNotFound) {
		_, err = core.js.AddStream(&nats.StreamConfig{
			Name:     "LOG",
			Subjects: []string{"log"},
		})
		if err != nil {
			t.Fatal(err)
		}
	} else if err != nil {
		t.Fatal(err)
	}
	logger := zap.New(core, zap.Development())
	logger.Warn("This is a test")
	con.Close()
}
