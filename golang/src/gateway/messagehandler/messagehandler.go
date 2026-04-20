package messagehandler

import (
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(inner.TypeData, data, "")
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeMessage(inner.TypeEOF, []fruititem.FruitItem{}, "")
}

// TODO: el caso de que el type sea EOF no es muy correcto devolver nil y nil en error,
// mas adelante buscar una mejor forma de manejar esto. Probablemente asegurando que el Join
// no envie mensajes EOF al GW, y que aca se. puedan tratar como error
func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	envelope, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if envelope.Type == inner.TypeEOF {
		return nil, nil
	}
	return envelope.Payload, nil
}
