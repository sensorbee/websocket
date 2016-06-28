package websocket

import (
	"fmt"
	"golang.org/x/net/websocket"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
	"strings"
	"time"
)

type wsReceiverSource struct {
	originURL string
	topology  string
	stream    string
	stopped   chan struct{}
}

func (r *wsReceiverSource) GenerateStream(ctx *core.Context, w core.Writer) error {
	var waitInterval time.Duration

	// loop for reconnection
	for {
		select {
		case <-r.stopped:
			return core.ErrSourceStopped
		case <-time.After(waitInterval):
		}
		waitInterval = time.Duration(2) * time.Second // TODO: exponential backoff

		if err, retryable := r.connectAndReceive(ctx, w); err == nil {
			// no error means that the source has received "eos"
			return nil
		} else if !retryable {
			return err
		}
	}
}

func (r *wsReceiverSource) connectAndReceive(ctx *core.Context, w core.Writer) (error, bool) {
	// connect to the given server
	wsURL := fmt.Sprintf("ws%s/api/v1/topologies/%s/wsqueries",
		strings.TrimPrefix(r.originURL, "http"), r.topology)
	ws, err := websocket.Dial(wsURL, "", r.originURL)
	if err != nil {
		ctx.ErrLog(err).Error("unable to connect to remote host")
		return err, true
	}
	defer ws.Close()
	ctx.Log().WithField("url", r.originURL).
		Info("connected to remote host")

	// send a command
	stmt := fmt.Sprintf("SELECT RSTREAM ts(), * AS data FROM %s [RANGE 1 TUPLES];",
		r.stream)
	msg := data.Map{
		"rid": data.Int(1),
		"payload": data.Map{
			"queries": data.String(stmt),
		},
	}
	if err := websocket.JSON.Send(ws, msg); err != nil {
		ctx.ErrLog(err).Error("failed to send query to remote host")
		return err, true
	}

	// receive the "start of stream" response
	first := data.Map{}
	if err := websocket.JSON.Receive(ws, &first); err != nil {
		ctx.ErrLog(err).Error("failed to receive/process sos message")
		return err, true
	}

	t, err := data.AsString(first["type"])
	if err != nil {
		ctx.ErrLog(err).WithField("type", first["type"]).
			Error("'type' value is not a string")
		return err, true
	}
	if t == "error" {
		err := fmt.Errorf("%s", first["payload"])
		ctx.ErrLog(err).Error("server returned error, not start-of-stream")
		return err, true
	} else if t != "sos" {
		typeErr := fmt.Errorf(`"type" was expected to be "sos", not "%s"`, t)
		ctx.ErrLog(typeErr).Error("wrong message type")
		return typeErr, true
	}
	return r.receive(ctx, ws, w)
}

func (r *wsReceiverSource) receive(ctx *core.Context, ws *websocket.Conn, w core.Writer) (error, bool) {
	for {
		select {
		case <-r.stopped:
			return core.ErrSourceStopped, false
		default:
		}

		// receive and parse the data
		d := data.Map{}
		if err := websocket.JSON.Receive(ws, &d); err != nil {
			ctx.ErrLog(err).Error("failed to receive/parse stream item")
			// TODO: change this check to a better way. It doesn't look stable.
			// Moreover, it doesn't stop when a critical error other than EOF is returned.
			if err.Error() == "EOF" {
				return err, true
			}
			continue
		}

		// check that this is an actual result
		payloadRaw := d["payload"]
		if rid, err := data.ToInt(d["rid"]); err != nil {
			ctx.ErrLog(err).WithField("rid", d["rid"]).
				Error("protocol violation: 'rid' was not an int")
			continue
		} else if rid != 1 {
			ctx.ErrLog(err).WithField("rid", d["rid"]).
				Error("protocol violation: 'rid' was not 1")
			continue
		}
		if t, err := data.AsString(d["type"]); err != nil {
			ctx.ErrLog(err).WithField("type", d["type"]).
				Error("protocol violation: 'type' was not a string")
			continue
		} else if t == "eos" {
			ctx.Log().Info("received end-of-stream message")
			return nil, false
		} else if t != "result" {
			ctx.ErrLog(fmt.Errorf("expected \"result\"-type message")).WithField("type", t).
				Error("received badly typed message")
			continue
		}

		// get the data out
		if payload, err := data.AsMap(payloadRaw); err != nil {
			ctx.ErrLog(err).WithField("payload", payloadRaw).
				Error("protocol violation: 'payload' was not an map")
			continue
		} else {
			// extract data
			contents, err := data.AsMap(payload["data"])
			if err != nil {
				ctx.ErrLog(err).WithField("data", payload["data"]).
					Error("malformed data: 'payload.data' was not a map")
				continue
			}
			// extract timestamp (we will reuse this later)
			ts_str, err := data.AsString(payload["ts"])
			if err != nil {
				ctx.ErrLog(err).WithField("ts", payload["ts"]).
					Error("malformed data: 'payload.ts' was not a string")
				continue
			}
			ts, err := data.ToTimestamp(data.String(ts_str))
			if err != nil {
				ctx.ErrLog(err).WithField("ts", payload["ts"]).
					Error("malformed data: 'payload.ts' was not a timestamp")
				continue
			}

			// now send the tuple. A tuple could be written after Stop
			// is called, but that it doesn't have to provide very
			// strict consistency.
			tup := core.NewTuple(contents)
			tup.Timestamp = ts
			tup.ProcTimestamp = ts
			if err = w.Write(ctx, tup); err != nil {
				return err, err != core.ErrSourceStopped
			}
		}
	}
}

func (r *wsReceiverSource) Stop(ctx *core.Context) error {
	select {
	case r.stopped <- struct{}{}:
	default:
	}
	return nil
}

func (r *wsReceiverSource) Status() data.Map {
	return data.Map{
		"url":      data.String(r.originURL),
		"topology": data.String(r.topology),
		"stream":   data.String(r.stream),
	}
}

func NewSource(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Source, error) {
	if params["topology"] == nil {
		return nil, fmt.Errorf("no topology given")
	}
	topology, err := data.AsString(params["topology"])
	if err != nil {
		return nil, fmt.Errorf("topology parameter must be a string")
	}

	if params["stream"] == nil {
		return nil, fmt.Errorf("no stream given")
	}
	stream, err := data.AsString(params["stream"])
	if err != nil {
		return nil, fmt.Errorf("stream parameter must be a string")
	}

	host := "localhost"
	if params["host"] != nil {
		host, err = data.AsString(params["host"])
		if err != nil {
			return nil, fmt.Errorf("host parameter must be a string")
		}
	}

	port := int64(15601)
	if params["port"] != nil {
		port, err = data.AsInt(params["port"])
		if err != nil {
			return nil, fmt.Errorf("port parameter must be an integer")
		}
	}

	return core.ImplementSourceStop(&wsReceiverSource{
		originURL: fmt.Sprintf("http://%s:%d", host, port),
		topology:  topology,
		stream:    stream,
		stopped:   make(chan struct{}, 1),
	}), nil
}
