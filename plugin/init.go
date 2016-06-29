package plugin

import (
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"pfi/sensorbee/websocket"
)

func init() {
	bql.MustRegisterGlobalSourceCreator("websocket", bql.SourceCreatorFunc(websocket.NewSource))
}
