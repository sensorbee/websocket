package websocket

import (
	"fmt"
	. "github.com/smartystreets/goconvey/convey"
	"pfi/sensorbee/sensorbee/bql"
	"pfi/sensorbee/sensorbee/core"
	"pfi/sensorbee/sensorbee/data"
	"testing"
)

/*func init() {
	bql.MustRegisterGlobalSourceCreator("remote",
		bql.SourceCreatorFunc(NewSource))
}*/

func TestRemoteSource(t *testing.T) {
	ctx := core.NewContext(nil)
	ioParams := &bql.IOParams{}

	Convey("Given a remote source", t, func() {
		src, err := NewSource(ctx, ioParams, data.Map{})
		So(err, ShouldBeNil)

		Convey("When generating a stream", func() {
			cnt := 0
			// this sink accepts only two tuples
			w := core.WriterFunc(func(ctx *core.Context, t *core.Tuple) error {
				cnt++
				if cnt == 2 {
					go src.Stop(ctx)
					return fmt.Errorf("limit reached")
				}
				return nil
			})
			err := src.GenerateStream(ctx, w)

			Convey("Then the connected writer should receive tuples", func() {
				So(err, ShouldBeNil)
			})
		})
	})
}
