package plugin

import (
	"gopkg.in/sensorbee/sensorbee.v0/bql"
	"gopkg.in/sensorbee/websocket.v1"
)

func init() {
	bql.MustRegisterGlobalSourceCreator("websocket", bql.SourceCreatorFunc(websocket.NewSource))
}
