package natzap

import (
	"errors"
	"fmt"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap/zapcore"
)

type Core struct {
	zapcore.LevelEnabler
	encoder zapcore.Encoder
	con     *nats.Conn
	js      nats.JetStreamContext
	subject string
}

func NewCore(enabler zapcore.LevelEnabler, encoder zapcore.Encoder, con *nats.Conn) *Core {
	return &Core{
		LevelEnabler: enabler,
		encoder:      encoder,
		con:          con,
		subject:      "",
		js:           nil,
	}
}

func (core *Core) WithSubject(subject string) *Core {
	core.subject = subject
	return core
}

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

func (core *Core) With(fields []zapcore.Field) zapcore.Core {
	clone := core.clone()
	for _, field := range fields {
		field.AddTo(clone.encoder)
	}
	return clone
}

func (core *Core) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if core.Enabled(entry.Level) {
		return checked.AddCore(entry, core)
	}
	return checked
}

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
