package natzap

import (
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
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
	info, err := core.js.StreamInfo(stream)
	if err != nil {
		return nil, err
	} else if info == nil {
		return nil, nats.ErrStreamNotFound
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
		return errors.Wrap(err, "failed to encode log entry")
	}
	defer buffer.Free()
	return core.con.Publish(core.subject, buffer.Bytes())
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
