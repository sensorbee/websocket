package websocket

import (
	"fmt"
	"golang.org/x/net/websocket"
	"pfi/sensorbee/sensorbee/bql"
	"pfi/sensorbee/sensorbee/core"
	"pfi/sensorbee/sensorbee/data"
	"sync/atomic"
	"time"
)

type remoteSensorBeeSource struct {
	host     string
	port     int
	topology string
	stream   string
	stopped  int32
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
		originURL := fmt.Sprintf("http://%s:%d", r.host, r.port)
		wsURL := fmt.Sprintf("ws://%s:%d/api/v1/topologies/%s/wsqueries",
			r.host, r.port, r.topology)
		ws, err := websocket.Dial(wsURL, "", originURL)
		// TODO log properly
		if err != nil {
			fmt.Printf("error during connect: %s\n", err.Error())
			continue
		} else {
			fmt.Printf("connected to %s\n", wsURL)
		}

		// send a command
		stmt := fmt.Sprintf("SELECT RSTREAM ts(), * AS data FROM %s [RANGE 1 TUPLES];", r.stream)
		fmt.Println(stmt)
		msg := data.Map{
			"rid": data.Int(1),
			"payload": data.Map{
				"queries": data.String(stmt),
			},
		}
		if err := websocket.JSON.Send(ws, msg); err != nil {
			// TODO log errors properly
			fmt.Printf("error during send: %s\n", err.Error())
			ws.Close()
			continue
		}

		// receive the "start of stream" response
		first := data.Map{}
		if err := websocket.JSON.Receive(ws, &first); err != nil {
			fmt.Printf("error during sos receive: %s\n", err.Error())
			ws.Close()
			continue
		} else {
			t, err := data.AsString(first["type"])
			if err != nil {
				// TODO log errors properly
				fmt.Printf("protocol violation: 'type' is not a string: %s\n", first["type"])
				ws.Close()
				continue
			}
			if t != "sos" {
				// TODO log errors properly
				fmt.Printf("received %s message (not \"sos\"): %s\n", first["type"], first["payload"])
				ws.Close()
				continue
			} else {
				fmt.Println("start of stream")
			}
		}

		// process the results
		for atomic.LoadInt32(&r.stopped) == 0 {
			// receive and parse the data
			d := data.Map{}
			if err := websocket.JSON.Receive(ws, &d); err != nil {
				// TODO log errors properly
				fmt.Printf("error while receiving data: %s\n", err.Error())
				continue
			}

			// check that this is an actual result
			payloadRaw := d["payload"]
			if rid, err := data.ToInt(d["rid"]); err != nil {
				// TODO log errors properly
				fmt.Printf("protocol violation: 'rid' was not an int: %s\n", d["rid"])
				continue
			} else if rid != 1 {
				// TODO log errors properly
				fmt.Printf("received message with rid %d (not 1): %s\n", rid, payloadRaw)
				continue
			}
			if t, err := data.AsString(d["type"]); err != nil {
				// TODO log errors properly
				fmt.Printf("protocol violation: 'type' was not an int: %s\n", d["type"])
				continue
			} else if t != "result" {
				// TODO log errors properly
				fmt.Printf("received '%s' message (not 'result'): %s\n", t, payloadRaw)
				continue
			}

			// get the data out
			if payload, err := data.AsMap(payloadRaw); err != nil {
				// TODO log errors properly
				fmt.Printf("protocol violation: 'payload' was not a map: %s\n", payloadRaw)
				continue
			} else {
				// extract data
				contents, err := data.AsMap(payload["data"])
				if err != nil {
					// TODO log errors properly
					fmt.Printf("malformed data: 'payload.data' was not a map: %s\n", payload["data"])
					continue
				}
				// extract timestamp (we will reuse this later)
				ts_str, err := data.AsString(payload["ts"])
				if err != nil {
					// TODO log errors properly
					fmt.Printf("malformed data: 'payload.ts' was not a string: %s\n", payload["ts"])
					continue
				}
				ts, err := data.ToTimestamp(data.String(ts_str))
				if err != nil {
					// TODO log errors properly
					fmt.Printf("malformed data: 'payload.ts' was not a timestamp: %s\n", payload["ts"])
					continue
				}

				// now send the tuple
				tup := &core.Tuple{
					Data:          contents,
					Timestamp:     ts,
					ProcTimestamp: ts,
				}
				if err = w.Write(ctx, tup); err != nil {
					// TODO log errors properly
					fmt.Printf("error while writing tuple: %s\n", err.Error())
				} else {
					fmt.Printf("wrote %v\n", *tup)
				}
			}
		}
	}
	return nil
}

func (r *remoteSensorBeeSource) Stop(ctx *core.Context) error {
	atomic.StoreInt32(&r.stopped, 1)
	return nil
}

func NewSource(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Source, error) {
	return &remoteSensorBeeSource{
		host:     "localhost",
		port:     8090,
		topology: "test",
		stream:   "foo",
	}, nil
}
