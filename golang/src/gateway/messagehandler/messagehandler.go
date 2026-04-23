package messagehandler

import (
	"fmt"
	"time"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageHandler struct {
	requestID string
}

func NewMessageHandler() MessageHandler {
	return MessageHandler{
		requestID: fmt.Sprintf("req-%d", time.Now().UnixNano()),
	}
}

func (messageHandler *MessageHandler) SerializeDataMessage(fruitRecord fruititem.FruitItem) (*middleware.Message, error) {
	data := []fruititem.FruitItem{fruitRecord}
	return inner.SerializeMessage(inner.TypeData, data, messageHandler.requestID, uint64(time.Now().UnixNano()))
}

func (messageHandler *MessageHandler) SerializeEOFMessage() (*middleware.Message, error) {
	return inner.SerializeMessage(inner.TypeEOF, []fruititem.FruitItem{}, messageHandler.requestID, uint64(time.Now().UnixNano()))
}

func (messageHandler *MessageHandler) DeserializeResultMessage(message *middleware.Message) ([]fruititem.FruitItem, error) {
	envelope, err := inner.DeserializeMessage(message)
	if err != nil {
		return nil, err
	}
	if envelope.Type != inner.TypeData {
		return nil, nil
	}
	return envelope.Payload, nil
}

func (messageHandler *MessageHandler) RequestID() string {
	return messageHandler.requestID
}
