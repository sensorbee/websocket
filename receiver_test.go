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
		get := func(m data.Map, p string) data.Value {
			v, err := m.Get(data.MustCompilePath(p))
			So(err, ShouldBeNil)
			return v
		}

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
					So(err.Error(), ShouldStartWith, "topology parameter must be a string")
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
						So(err.Error(), ShouldStartWith, "stream parameter must be a string")
					})
				})

				Convey("When the type is correct", func() {
					params["stream"] = data.String("bar")
					Convey("Then construction succeeds", func() {
						src, err := NewSource(ctx, ioParams, params)
						So(err, ShouldBeNil)
						s := src.(core.Statuser).Status()
						So(get(s, "internal_source.topology"), ShouldEqual, "foo")
						So(get(s, "internal_source.stream"), ShouldEqual, "bar")
						So(get(s, "internal_source.url"), ShouldEqual, "http://localhost:15601")
					})

					Convey("And when host is given", func() {
						Convey("When the type is wrong", func() {
							params["host"] = data.Int(7)
							Convey("Then construction fails", func() {
								_, err := NewSource(ctx, ioParams, params)
								So(err, ShouldNotBeNil)
								So(err.Error(), ShouldStartWith, "host parameter must be a string")
							})
						})

						Convey("When the type is correct", func() {
							params["host"] = data.String("example.com")
							Convey("Then host information is set", func() {
								src, err := NewSource(ctx, ioParams, params)
								So(err, ShouldBeNil)
								s := src.(core.Statuser).Status()
								So(get(s, "internal_source.topology"), ShouldEqual, "foo")
								So(get(s, "internal_source.stream"), ShouldEqual, "bar")
								So(get(s, "internal_source.url"), ShouldEqual, "http://example.com:15601")
							})
						})
					})

					Convey("And when port is given", func() {
						Convey("When the type is wrong", func() {
							params["port"] = data.String("moge")
							Convey("Then construction fails", func() {
								_, err := NewSource(ctx, ioParams, params)
								So(err, ShouldNotBeNil)
								So(err.Error(), ShouldStartWith, "port parameter must be an integer")
							})
						})

						Convey("When the type is correct", func() {
							params["port"] = data.Int(1234)
							Convey("Then port information is set", func() {
								src, err := NewSource(ctx, ioParams, params)
								So(err, ShouldBeNil)
								s := src.(core.Statuser).Status()
								So(get(s, "internal_source.topology"), ShouldEqual, "foo")
								So(get(s, "internal_source.stream"), ShouldEqual, "bar")
								So(get(s, "internal_source.url"), ShouldEqual, "http://localhost:1234")
							})
						})
					})
				})
			})
		})

		Convey("When buffer_size is given", func() {
			params := data.Map{
				"topology": data.String("foo"),
				"stream":   data.String("bar"),
			}
			Convey("When the value is correct", func() {
				params["buffer_size"] = data.Int(10)
				Convey("Then construction succeeds", func() {
					src, err := NewSource(ctx, ioParams, params)
					So(err, ShouldBeNil)
					s := src.(core.Statuser).Status()
					So(get(s, "internal_source.buffer_size"), ShouldEqual, 10)
				})
			})

			Convey("When the value is invalid", func() {
				params["buffer_size"] = data.Int(-1)
				Convey("Then construction fails", func() {
					_, err := NewSource(ctx, ioParams, params)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldStartWith, "invalid buffer_size")
				})
			})

			Convey("When the type is wrong", func() {
				params["buffer_size"] = data.String("10")
				Convey("Then construction fails", func() {
					_, err := NewSource(ctx, ioParams, params)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldStartWith, "buffer_size parameter must be an integer")
				})
			})
		})

		Convey("When drop_mode is given", func() {
			params := data.Map{
				"topology": data.String("foo"),
				"stream":   data.String("bar"),
			}

			for _, v := range []string{"wait", "newest", "oldest"} {
				Convey(fmt.Sprintf("When the value is %v", v), func() {
					params["drop_mode"] = data.String(v)
					Convey("Then construction succeeds", func() {
						src, err := NewSource(ctx, ioParams, params)
						So(err, ShouldBeNil)
						s := src.(core.Statuser).Status()
						So(get(s, "internal_source.drop_mode"), ShouldEqual, v)
					})
				})
			}

			Convey("When the value is invalid", func() {
				params["drop_mode"] = data.String("invalid")
				Convey("Then construction fails", func() {
					_, err := NewSource(ctx, ioParams, params)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldStartWith, "invalid drop_mode value")
				})
			})

			Convey("When the type is wrong", func() {
				params["drop_mode"] = data.Int(10)
				Convey("Then construction fails", func() {
					_, err := NewSource(ctx, ioParams, params)
					So(err, ShouldNotBeNil)
					So(err.Error(), ShouldStartWith, "drop_mode parameter must be a string")
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
					time.Sleep(10 * time.Millisecond)
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
				wg.Add(1)
				go func() {
					defer wg.Done()
					src.GenerateStream(ctx, w)
				}()

				// we should get no tuples initially since the remote stream
				// does not exist. Although this check doesn't always run after
				// GenerateStream is called, it should sometimes fails if the
				// code has some problem.
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
					time.Sleep(10 * time.Millisecond)
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

func newTestSource(ctx *core.Context, url string, topology string, stream string) (core.Source, error) {
	return core.ImplementSourceStop(&wsReceiverSource{
		originURL:      url,
		topology:       topology,
		stream:         stream,
		bufferSize:     1024,
		dropMode:       "wait",
		dropModeClause: "WAIT",
		stopped:        make(chan struct{}, 1),
	}), nil
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
}

func (d *dummySource) GenerateStream(ctx *core.Context, w core.Writer) error {
	for i := 0; ; i++ {
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
		time.Sleep(1 * time.Millisecond) // This sleep makes the test faster
	}
	return nil
}

func (d *dummySource) Stop(ctx *core.Context) error {
	return nil
}

func createDummySource(ctx *core.Context, ioParams *bql.IOParams, params data.Map) (core.Source, error) {
	return core.ImplementSourceStop(&dummySource{}), nil
}
