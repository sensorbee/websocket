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
	originURL      string
	topology       string
	stream         string
	bufferSize     int
	dropMode       string
	dropModeClause string
	stopped        chan struct{}
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

		if retryable, err := r.connectAndReceive(ctx, w); err == nil {
			// no error means that the source has received "eos"
			return nil
		} else if !retryable {
			return err
		}
	}
}

func (r *wsReceiverSource) connectAndReceive(ctx *core.Context, w core.Writer) (bool, error) {
	// connect to the given server
	wsURL := fmt.Sprintf("ws%s/api/v1/topologies/%s/wsqueries",
		strings.TrimPrefix(r.originURL, "http"), r.topology)
	ws, err := websocket.Dial(wsURL, "", r.originURL)
	if err != nil {
		ctx.ErrLog(err).Error("unable to connect to remote host")
		return true, err
	}
	defer ws.Close()
	ctx.Log().WithField("url", r.originURL).
		Info("connected to remote host")

	// send a command
	stmt := fmt.Sprintf("SELECT RSTREAM ts(), * AS data FROM %s [RANGE 1 TUPLES, BUFFER SIZE %v, %v IF FULL];",
		r.stream, r.bufferSize, r.dropModeClause)
	msg := data.Map{
		"rid": data.Int(1),
		"payload": data.Map{
			"queries": data.String(stmt),
		},
	}
	if err := websocket.JSON.Send(ws, msg); err != nil {
		ctx.ErrLog(err).Error("failed to send query to remote host")
		return true, err
	}

	// receive the "start of stream" response
	first := data.Map{}
	if err := websocket.JSON.Receive(ws, &first); err != nil {
		ctx.ErrLog(err).Error("failed to receive/process sos message")
		return true, err
	}

	t, err := data.AsString(first["type"])
	if err != nil {
		ctx.ErrLog(err).WithField("type", first["type"]).
			Error("'type' value is not a string")
		return true, err
	}
	if t == "error" {
		err := fmt.Errorf("%s", first["payload"])
		ctx.ErrLog(err).Error("server returned error, not start-of-stream")
		return true, err
	} else if t != "sos" {
		typeErr := fmt.Errorf(`"type" was expected to be "sos", not "%s"`, t)
		ctx.ErrLog(typeErr).Error("wrong message type")
		return true, typeErr
	}
	return r.receive(ctx, ws, w)
}

func (r *wsReceiverSource) receive(ctx *core.Context, ws *websocket.Conn, w core.Writer) (bool, error) {
	for {
		select {
		case <-r.stopped:
			return false, core.ErrSourceStopped
		default:
		}

		// receive and parse the data
		d := data.Map{}
		if err := websocket.JSON.Receive(ws, &d); err != nil {
			ctx.ErrLog(err).Error("failed to receive/parse stream item")
			// TODO: change this check to a better way. It doesn't look stable.
			// Moreover, it doesn't stop when a critical error other than EOF is returned.
			if err.Error() == "EOF" {
				return true, err
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
			return false, nil
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
			tsStr, err := data.AsString(payload["ts"])
			if err != nil {
				ctx.ErrLog(err).WithField("ts", payload["ts"]).
					Error("malformed data: 'payload.ts' was not a string")
				continue
			}
			ts, err := data.ToTimestamp(data.String(tsStr))
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
				return err != core.ErrSourceStopped, err
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
		"url":         data.String(r.originURL),
		"topology":    data.String(r.topology),
		"stream":      data.String(r.stream),
		"buffer_size": data.Int(r.bufferSize),
		"drop_mode":   data.String(r.dropMode),
	}
}

// NewSource creates a new WebSocket source to connect to a remote SensorBee
// instance. The source has following required parameters:
//
//	* topology: the name of the topology in the remote instance
//	* stream: the name of the stream to issue a query
//
// It also has optional parameters:
//
//	* host: the name of the host (default: "localhost")
//	* port: the port number of the remote instance (default: 15601)
//	* buffer_size: the buffer size in the BUFFER SIZE clause (default: 1024)
//	* drop_mode: the behavior of the IF FULL clause: "wait", "newest", or "oldest" (default: "wait")
func NewSource(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Source, error) {
	if params["topology"] == nil {
		return nil, fmt.Errorf("no topology given")
	}
	topology, err := data.AsString(params["topology"])
	if err != nil {
		return nil, fmt.Errorf("topology parameter must be a string: %v", err)
	}

	if params["stream"] == nil {
		return nil, fmt.Errorf("no stream given")
	}
	stream, err := data.AsString(params["stream"])
	if err != nil {
		return nil, fmt.Errorf("stream parameter must be a string: %v", err)
	}

	host := "localhost"
	if params["host"] != nil {
		host, err = data.AsString(params["host"])
		if err != nil {
			return nil, fmt.Errorf("host parameter must be a string: %v", err)
		}
	}

	port := int64(15601)
	if params["port"] != nil {
		port, err = data.AsInt(params["port"])
		if err != nil {
			return nil, fmt.Errorf("port parameter must be an integer: %v", err)
		}
	}

	bufferSize := 1024
	if v, ok := params["buffer_size"]; ok {
		s, err := data.AsInt(v)
		if err != nil {
			return nil, fmt.Errorf("buffer_size parameter must be an integer: %v", err)
		}
		c := core.BoxInputConfig{}
		c.Capacity = int(s)
		if err := c.Validate(); err != nil {
			return nil, fmt.Errorf("invalid buffer_size: %v", err)
		}
		bufferSize = int(s)
	}

	drop := "wait"
	dropClause := "WAIT"
	if v, ok := params["drop_mode"]; ok {
		drop, err = data.AsString(v)
		if err != nil {
			return nil, fmt.Errorf("drop_mode parameter must be a string: %v", err)
		}

		switch drop {
		case "wait":
			dropClause = "WAIT"
		case "newest":
			dropClause = "DROP NEWEST"
		case "oldest":
			dropClause = "DROP OLDEST"
		default:
			return nil, fmt.Errorf("invalid drop_mode value: %v", drop)
		}
	}
	return core.ImplementSourceStop(&wsReceiverSource{
		originURL:      fmt.Sprintf("http://%s:%d", host, port),
		topology:       topology,
		stream:         stream,
		bufferSize:     bufferSize,
		dropMode:       drop,
		dropModeClause: dropClause,
		stopped:        make(chan struct{}, 1),
	}), nil
}
