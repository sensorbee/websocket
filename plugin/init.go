package plugin

import (
	"pfi/sensorbee/sensorbee/bql"
	"pfi/sensorbee/websocket"
)

func init() {
	bql.MustRegisterGlobalSourceCreator("websocket", bql.SourceCreatorFunc(websocket.NewSource))
}
