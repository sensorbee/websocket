package websocket

import (
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
	"pfi/sensorbee/sensorbee/bql"
	"pfi/sensorbee/sensorbee/client"
	"pfi/sensorbee/sensorbee/core"
	"pfi/sensorbee/sensorbee/data"
	"pfi/sensorbee/sensorbee/server/testutil"
	"testing"
	"time"
)

func init() {
	bql.MustRegisterGlobalSourceCreator("dummy", bql.SourceCreatorFunc(createDummySource))
}

func TestRemoteSource(t *testing.T) {
	// start a test server
	currentRealHTTPValue := testutil.TestAPIWithRealHTTPServer
	testutil.TestAPIWithRealHTTPServer = true
	srv := testutil.NewServer()
	defer func() {
		// reset to previous value for other tests
		testutil.TestAPIWithRealHTTPServer = currentRealHTTPValue
		srv.Close()
	}()
	// get a client for this server
	r, err := client.NewRequesterWithClient(srv.URL(), "v1", srv.HTTPClient())
	if err != nil {
		panic(err)
	}

	Convey("Given a remote source with a topology and a stream", t, func() {
		// create a topology and a source on the server
		res, err := r.Do(client.Post, "/topologies", map[string]interface{}{
			"name": "test",
		})
		So(err, ShouldBeNil)
		So(res.Raw.StatusCode, ShouldEqual, http.StatusOK)
		res, err = r.Do(client.Post, "/topologies/test/queries", map[string]interface{}{
			"queries": `CREATE SOURCE foo TYPE dummy;`,
		})
		So(err, ShouldBeNil)
		So(res.Raw.StatusCode, ShouldEqual, http.StatusOK)
		// clean up after every run
		Reset(func() {
			r.Do(client.Post, "/topologies/test/queries", map[string]interface{}{
				"queries": `DROP SOURCE foo;`,
			})
			r.Do(client.Delete, "/topologies/test", nil)
		})

		Convey("When connecting to that remote stream", func() {
			ctx := core.NewContext(nil)
			src, err := newTestSource(ctx, srv.URL())
			So(err, ShouldBeNil)

			Convey("Then we should receive tuples from there", func() {
				// this Writer stops the receiving source after 10 tuples
				cnt := 0
				w := core.WriterFunc(func(ctx *core.Context, t *core.Tuple) error {
					cnt++
					if cnt == 10 {
						go src.Stop(ctx)
						return nil
					}
					return nil
				})
				err := src.GenerateStream(ctx, w)

				So(err, ShouldBeNil)
				So(cnt, ShouldBeGreaterThanOrEqualTo, 10)
			})
		})
	})
}

type dummySource struct {
	stopped bool
}

func (d *dummySource) GenerateStream(ctx *core.Context, w core.Writer) error {
	for i := 0; !d.stopped && i < 99999999999999; i++ {
		now := time.Now()
		if err := w.Write(ctx, &core.Tuple{
			Data: data.Map{
				"int": data.Int(i),
			},
			Timestamp:     now,
			ProcTimestamp: now,
		}); err != nil {
			return err
		}
		time.Sleep(time.Duration(10) * time.Millisecond)
	}
	return nil
}

func (d *dummySource) Stop(ctx *core.Context) error {
	d.stopped = true
	return nil
}

func createDummySource(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Source, error) {
	return &dummySource{}, nil
}
