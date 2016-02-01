package websocket

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"net/http"
	"pfi/sensorbee/sensorbee/bql"
	"pfi/sensorbee/sensorbee/client"
	"pfi/sensorbee/sensorbee/core"
	"pfi/sensorbee/sensorbee/data"
	"pfi/sensorbee/sensorbee/server/testutil"
	"sync"
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
		err = createRemoteDummyStream(r, "foo")
		So(err, ShouldBeNil)
		// clean up after every run
		Reset(func() {
			dropRemoteDummyStream(r, "foo")
			r.Do(client.Delete, "/topologies/test", nil)
		})

		ctx := core.NewContext(nil)

		Convey("When connecting to that remote stream", func() {
			src, err := newTestSource(ctx, srv.URL(), "test", "foo")
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

		Convey("When connecting to a non-existing remote stream", func() {
			src, err := newTestSource(ctx, srv.URL(), "test", "foo2")
			So(err, ShouldBeNil)

			Convey("Then we should receive no tuples from there", func() {
				// stop the receiving source after some time
				go func() {
					time.Sleep(100 * time.Millisecond)
					go src.Stop(ctx)
				}()
				cnt := 0
				w := core.WriterFunc(func(ctx *core.Context, t *core.Tuple) error {
					cnt++
					return nil
				})
				err := src.GenerateStream(ctx, w)

				So(err, ShouldBeNil)
				So(cnt, ShouldEqual, 0)
			})

			Convey("If it is created later then we should receive some tuples", func() {
				cnt := 0
				w := core.WriterFunc(func(ctx *core.Context, t *core.Tuple) error {
					cnt++
					if cnt == 10 {
						go src.Stop(ctx)
						return nil
					}
					return nil
				})

				wg := sync.WaitGroup{}
				go func() {
					defer wg.Done()
					wg.Add(1)
					src.GenerateStream(ctx, w)
				}()

				// get no tuples within 100ms
				time.Sleep(100 * time.Millisecond)
				So(cnt, ShouldEqual, 0)

				err = createRemoteDummyStream(r, "foo2")
				So(err, ShouldBeNil)

				// wait until GenerateStream exits
				wg.Wait()
				So(cnt, ShouldBeGreaterThanOrEqualTo, 10)

				dropRemoteDummyStream(r, "foo2")
			})
		})

		Convey("When connecting to a non-existing remote topology", func() {
			src, err := newTestSource(ctx, srv.URL(), "test2", "foo")
			So(err, ShouldBeNil)

			Convey("Then we should receive no tuples from there", func() {
				// stop the receiving source after some time
				go func() {
					time.Sleep(100 * time.Millisecond)
					go src.Stop(ctx)
				}()
				cnt := 0
				w := core.WriterFunc(func(ctx *core.Context, t *core.Tuple) error {
					cnt++
					return nil
				})
				err := src.GenerateStream(ctx, w)

				So(err, ShouldBeNil)
				So(cnt, ShouldEqual, 0)
			})
		})
	})
}

func createRemoteDummyStream(r *client.Requester, streamName string) error {
	res, err := r.Do(client.Post, "/topologies/test/queries", map[string]interface{}{
		"queries": fmt.Sprintf(`CREATE SOURCE %s TYPE dummy;`, streamName),
	})
	if err != nil {
		return err
	}
	if res.Raw.StatusCode != http.StatusOK {
		return fmt.Errorf("status code is %d, not %d",
			res.Raw.StatusCode, http.StatusOK)
	}
	return nil
}

func dropRemoteDummyStream(r *client.Requester, streamName string) error {
	res, err := r.Do(client.Post, "/topologies/test/queries", map[string]interface{}{
		"queries": fmt.Sprintf(`DROP SOURCE %s;`, streamName),
	})
	if err != nil {
		return err
	}
	if res.Raw.StatusCode != http.StatusOK {
		return fmt.Errorf("status code is %d, not %d",
			res.Raw.StatusCode, http.StatusOK)
	}
	return nil
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
		time.Sleep(10 * time.Millisecond)
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
