package websocket

import (
	"fmt"
	"golang.org/x/net/websocket"
	"pfi/sensorbee/sensorbee/bql"
	"pfi/sensorbee/sensorbee/core"
	"pfi/sensorbee/sensorbee/data"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type remoteSensorBeeSource struct {
	originURL string
	topology  string
	stream    string
	stopped   int32
	writeMut  sync.Mutex
}

func (r *remoteSensorBeeSource) GenerateStream(ctx *core.Context, w core.Writer) error {
	firstRun := true
	// unless the UDSF is stopped, try to reconnect
	for atomic.LoadInt32(&r.stopped) == 0 {
		// wait if this is not the first time
		if !firstRun {
			time.Sleep(time.Duration(2) * time.Second)
		}
		firstRun = false

		// connect to the given server
		wsURL := fmt.Sprintf("ws%s/api/v1/topologies/%s/wsqueries",
			strings.TrimPrefix(r.originURL, "http"), r.topology)
		ws, err := websocket.Dial(wsURL, "", r.originURL)
		if err != nil {
			ctx.ErrLog(err).Error("unable to connect to remote host")
			continue
		} else {
			ctx.Log().WithField("url", r.originURL).
				Info("connected to remote host")
		}

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
			ws.Close()
			continue
		}

		// receive the "start of stream" response
		first := data.Map{}
		if err := websocket.JSON.Receive(ws, &first); err != nil {
			ctx.ErrLog(err).Error("failed to receive/process sos message")
			ws.Close()
			continue
		} else {
			t, err := data.AsString(first["type"])
			if err != nil {
				ctx.ErrLog(err).WithField("type", first["type"]).
					Error("'type' value is not a string")
				ws.Close()
				continue
			}
			if t == "error" {
				ctx.ErrLog(fmt.Errorf("%s", first["payload"])).
					Error("server returned error, not start-of-stream")
				continue
			} else if t != "sos" {
				typeErr := fmt.Errorf(`"type" was expected to be "sos", not "%s"`, t)
				ctx.ErrLog(typeErr).Error("wrong message type")
				ws.Close()
				continue
			}
		}

		// process the results
		for atomic.LoadInt32(&r.stopped) == 0 {
			// receive and parse the data
			d := data.Map{}
			if err := websocket.JSON.Receive(ws, &d); err != nil {
				ctx.ErrLog(err).Error("failed to receive/parse stream item")
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
				// we end the processing here
				atomic.StoreInt32(&r.stopped, 1)
				break
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

				// now send the tuple
				tup := &core.Tuple{
					Data:          contents,
					Timestamp:     ts,
					ProcTimestamp: ts,
				}
				func() {
					// We wrap the `Write` call with the check for
					// the stopped flag in a mutex, so that the flag
					// is set *either* before *or* after the `Write`,
					// not somewhere in between.
					r.writeMut.Lock()
					defer r.writeMut.Unlock()
					if atomic.LoadInt32(&r.stopped) == 0 {
						if err = w.Write(ctx, tup); err != nil {
							ctx.ErrLog(err).Error("failed to write tuple")
						}
					}
				}()
			}
		}
		// if we arrive here, then the receiving process
		// has been stopped
		ws.Close()
	}
	return nil
}

func (r *remoteSensorBeeSource) Stop(ctx *core.Context) error {
	// We set the stop flag, but we must make sure that
	// after this function has returned, no more calls to
	// `Write` are made. Therefore both this "set stopped flag"
	// block and the "write tuple if not stopped" block are
	// wrapped in a mutex.
	r.writeMut.Lock()
	defer r.writeMut.Unlock()
	atomic.StoreInt32(&r.stopped, 1)
	return nil
}

func NewSource(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Source, error) {
	return &remoteSensorBeeSource{
		originURL: "http://localhost:8090",
		topology:  "test",
		stream:    "foo",
	}, nil
}

func newTestSource(ctx *core.Context, url string, topology string, stream string) (core.Source, error) {
	return &remoteSensorBeeSource{
		originURL: url,
		topology:  topology,
		stream:    stream,
	}, nil
}
