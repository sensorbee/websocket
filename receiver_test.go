package websocket

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/sensorbee.v0/client"
	"gopkg.in/sensorbee/sensorbee.v0/core"
	"gopkg.in/sensorbee/sensorbee.v0/data"
	"gopkg.in/sensorbee/sensorbee.v0/server/testutil"
	"net/http"
	"sync"
	"testing"
	"time"
)

func init() {
	bql.MustRegisterGlobalSourceCreator("dummy", bql.SourceCreatorFunc(createDummySource))
}

func TestRemoteSourceParameters(t *testing.T) {
	ctx := core.NewContext(nil)
	ioParams := &bql.IOParams{}

	Convey("Given a source constructor", t, func() {
		Convey("When no parameters are given", func() {
			Convey("Then construction fails", func() {
				params := data.Map{}
				_, err := NewSource(ctx, ioParams, params)
				So(err, ShouldNotBeNil)
				So(err.Error(), ShouldEqual, "no topology given")
			})
		})

		Convey("When topology is given", func() {
			params := data.Map{}
			Convey("When the type is wrong", func() {
				params["topology"] = data.Int(7)
				Convey("Then construction fails", func() {
					_, err := NewSource(ctx, ioParams, params)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "topology parameter must be a string")
				})
			})

			Convey("When the type is correct but nothing else is given", func() {
				params["topology"] = data.String("foo")
				Convey("Then construction fails", func() {
					_, err := NewSource(ctx, ioParams, params)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldEqual, "no stream given")
				})
			})

			Convey("And stream is given", func() {
				params["topology"] = data.String("foo")
				Convey("When the type is wrong", func() {
					params["stream"] = data.Int(7)
					Convey("Then construction fails", func() {
						_, err := NewSource(ctx, ioParams, params)
						So(err, ShouldNotBeNil)
						So(err.Error(), ShouldEqual, "stream parameter must be a string")
					})
				})

				Convey("When the type is correct", func() {
					params["stream"] = data.String("bar")
					Convey("Then construction succeeds", func() {
						src, err := NewSource(ctx, ioParams, params)
						So(err, ShouldBeNil)
						So(src, ShouldHaveSameTypeAs, &wsReceiverSource{})
						s := src.(*wsReceiverSource)
						So(s.topology, ShouldEqual, "foo")
						So(s.stream, ShouldEqual, "bar")
						So(s.originURL, ShouldEqual, "http://localhost:15601")
					})

					Convey("And when host is given", func() {
						Convey("When the type is wrong", func() {
							params["host"] = data.Int(7)
							Convey("Then construction fails", func() {
								_, err := NewSource(ctx, ioParams, params)
								So(err, ShouldNotBeNil)
								So(err.Error(), ShouldEqual, "host parameter must be a string")
							})
						})

						Convey("When the type is correct", func() {
							params["host"] = data.String("example.com")
							Convey("Then host information is set", func() {
								src, err := NewSource(ctx, ioParams, params)
								So(err, ShouldBeNil)
								So(src, ShouldHaveSameTypeAs, &wsReceiverSource{})
								s := src.(*wsReceiverSource)
								So(s.topology, ShouldEqual, "foo")
								So(s.stream, ShouldEqual, "bar")
								So(s.originURL, ShouldEqual, "http://example.com:15601")
							})
						})
					})

					Convey("And when port is given", func() {
						Convey("When the type is wrong", func() {
							params["port"] = data.String("moge")
							Convey("Then construction fails", func() {
								_, err := NewSource(ctx, ioParams, params)
								So(err, ShouldNotBeNil)
								So(err.Error(), ShouldEqual, "port parameter must be an integer")
							})
						})

						Convey("When the type is correct", func() {
							params["port"] = data.Int(1234)
							Convey("Then port information is set", func() {
								src, err := NewSource(ctx, ioParams, params)
								So(err, ShouldBeNil)
								So(src, ShouldHaveSameTypeAs, &wsReceiverSource{})
								s := src.(*wsReceiverSource)
								So(s.topology, ShouldEqual, "foo")
								So(s.stream, ShouldEqual, "bar")
								So(s.originURL, ShouldEqual, "http://localhost:1234")
							})
						})
					})
				})
			})
		})
	})
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
		Reset(func() {
			dropRemoteDummyStream(r, "foo")
			r.Do(client.Delete, "/topologies/test", nil)
		})

		ctx := core.NewContext(nil)

		Convey("When connecting to that remote stream", func() {
			src, err := newTestSource(ctx, srv.URL(), "test", "foo")
			So(err, ShouldBeNil)

			Convey("Then we should receive tuples from there", func() {
				// a Writer to receive the tuples from the source
				cnt := 0
				w := core.WriterFunc(func(ctx *core.Context, t *core.Tuple) error {
					cnt++
					// this Writer stops the receiving source after 10 tuples
					if cnt == 10 {
						// call src.Stop() in a goroutine to avoid a deadlock
						go src.Stop(ctx)
						return nil
					}
					return nil
				})

				err := src.GenerateStream(ctx, w)
				So(err, ShouldBeNil)
				So(cnt, ShouldBeGreaterThanOrEqualTo, 10)
			})

			Convey("And when that stream goes away after some time", func() {
				// a Writer to receive the tuples from the source
				cnt := 0
				w := core.WriterFunc(func(ctx *core.Context, t *core.Tuple) error {
					cnt++
					if cnt == 10 {
						// this Writer drops the remote stream after 10 tuples
						go dropRemoteDummyStream(r, "foo")
						return nil
					}
					return nil
				})

				Convey("Then the source should stop itself", func() {
					// GenerateStream exits if an "eos" message is received
					err := src.GenerateStream(ctx, w)
					So(err, ShouldBeNil)
					So(cnt, ShouldBeGreaterThanOrEqualTo, 10)
				})
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

				// a Writer to receive the tuples from the source
				cnt := 0
				w := core.WriterFunc(func(ctx *core.Context, t *core.Tuple) error {
					cnt++
					return nil
				})

				// GenerateStream should exit when the source is stopped,
				// and we shouldn't have received any tuples
				err := src.GenerateStream(ctx, w)
				So(err, ShouldBeNil)
				So(cnt, ShouldEqual, 0)
			})

			Convey("If it is created later then we should receive some tuples", func() {
				// a Writer to receive the tuples from the source
				cnt := 0
				w := core.WriterFunc(func(ctx *core.Context, t *core.Tuple) error {
					cnt++
					if cnt == 10 {
						// this Writer stops the receiving source after 10 tuples
						go src.Stop(ctx)
						return nil
					}
					return nil
				})

				// launch GenerateStream in the background
				wg := sync.WaitGroup{}
				go func() {
					defer wg.Done()
					wg.Add(1)
					src.GenerateStream(ctx, w)
				}()

				// we should get no tuples initially since the remote stream
				// does not exist
				time.Sleep(100 * time.Millisecond)
				So(cnt, ShouldEqual, 0)

				// create the remote stream
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

				// a Writer to receive the tuples from the source
				cnt := 0
				w := core.WriterFunc(func(ctx *core.Context, t *core.Tuple) error {
					cnt++
					return nil
				})

				// GenerateStream should exit when the source is stopped,
				// and we shouldn't have received any tuples
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
