// Package natzap implements a zap core which sends logs over a NATS connection
// via NATS core or NATS JetStream.
package natzap

import (
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap/zapcore"
)

// Core implements the zapcore.Core interface.
// Allow logs to be sent via NATS
type Core struct {
	zapcore.LevelEnabler
	encoder zapcore.Encoder
	con     *nats.Conn
	js      nats.JetStreamContext
	subject string
}

// NewCore creates a new minimal Core instance
//
// Example:
//
//	con, _ := nats.Connect(nats.DefaultURL)
//	encoder := zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig())
//	core := natzap.NewCore(zapcore.WarnLevel, encoder, con)
func NewCore(enabler zapcore.LevelEnabler, encoder zapcore.Encoder, con *nats.Conn) *Core {
	return &Core{
		LevelEnabler: enabler,
		encoder:      encoder,
		con:          con,
		subject:      "",
		js:           nil,
	}
}

// WithSubject add a subject to a Core instance and returns the same instance
//
// Example:
//
//	con, _ := nats.Connect(nats.DefaultURL)
//	encoder := zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig())
//	core := natzap.NewCore(zapcore.WarnLevel, encoder, con).WithSubject("foo.bar")
func (core *Core) WithSubject(subject string) *Core {
	core.subject = subject
	return core
}

// WithJetStream adds JestStream communication to a Core instance.
//
// Returns the same Core instance and nil if the stream exists, the Core instance and a nats.ErrStreamNotFound error
// if the stream was not found, or a nil Core instance and error otherwise.
//
// Note: WithJetStream requires WithSubject to be called before it.
//
// Example:
//
//	con, err := nats.Connect(nats.DefaultURL)
//	if err != nil {
//	    log.Panic(err)
//	}
//	encoder := zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig())
//	core, err := NewCore(zapcore.WarnLevel, encoder, con).WithSubject("foo.bar").WithJetStream("FOO")
//	if errors.Is(err, nats.ErrStreamNotFound) {
//		_, err = core.js.AddStream(&nats.StreamConfig{
//		    Name:     "FOO",
//		    Subjects: []string{"foo.bar"},
//		})
//		if err != nil {
//		    log.Fatal(err)
//		}
//	} else if err != nil {
//	    log.Fatal(err)
//	}
func (core *Core) WithJetStream(stream string) (c *Core, err error) {
	core.js, err = core.con.JetStream()
	if err != nil {
		return nil, err
	}
	_, err = core.js.StreamInfo(stream)
	if errors.Is(err, nats.ErrStreamNotFound) {
		return core, err
	} else if err != nil {
		return nil, err
	}
	return core, nil
}

// With clones a Core instance and adds zapcore.Field instances to it
func (core *Core) With(fields []zapcore.Field) zapcore.Core {
	clone := core.clone()
	for _, field := range fields {
		field.AddTo(clone.encoder)
	}
	return clone
}

// Check checks if the log needs to be sent by checking the entry's level. This method is called by zap.
//
// Returns a zapcore.CheckedEntry instance reference with the Core added to it if the log is to be sent,
// or without the Core instance if it needs to be ignored
func (core *Core) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if core.Enabled(entry.Level) {
		return checked.AddCore(entry, core)
	}
	return checked
}

// Write writes a log message to the configured NATS subject or stream. This method is called by zap.
//
// Returns an error when the message cannot be encoded, when the NATS message is not published or
// when the message does not receive an ACK from the NATS server; otherwise returns nil
//
// Example:
//
//	con, err := nats.Connect(nats.DefaultURL)
//	if err != nil {
//	  t.Error(err)
//	}
//	encoder := zapcore.NewJSONEncoder(zap.NewDevelopmentEncoderConfig())
//	core := NewCore(zapcore.WarnLevel, encoder, con).WithSubject("foo.bar")
//	logger := zap.New(core, zap.Development())
//	logger.Warn("Warning message")
//	con.Close()
func (core *Core) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	buffer, err := core.encoder.EncodeEntry(entry, fields)
	if err != nil {
		return fmt.Errorf("%v: failed to encode log entry", err)
	}
	defer buffer.Free()
	if core.js != nil {
		ack, err := core.js.Publish(core.subject, buffer.Bytes())
		if err != nil || ack == nil {
			return err
		}
		return nil
	} else {
		return core.con.Publish(core.subject, buffer.Bytes())
	}
}

func (core *Core) Sync() error {
	return nil
}

func (core *Core) clone() *Core {
	return &Core{
		LevelEnabler: core.LevelEnabler,
		encoder:      core.encoder.Clone(),
		con:          core.con,
	}
}
