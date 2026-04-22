package inner

import (
	"encoding/json"
	"errors"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type MessageType string

const (
	TypeData   MessageType = "data"
	TypeEOF    MessageType = "eof"
	TypeSumEOF MessageType = "sum_eof"
)

type Envelope struct {
	Type      MessageType           `json:"type"`
	Payload   []fruititem.FruitItem `json:"payload,omitempty"`
	RequestID string                `json:"request_id,omitempty"`
	Sequence  uint64                `json:"sequence,omitempty"`
}

func serializeJSON(message Envelope) ([]byte, error) {
	return json.Marshal(message)
}

func deserializeJSON(message []byte) (*Envelope, error) {
	var data Envelope
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, err
	}
	return &data, nil
}

func SerializeMessage(messageType MessageType, payload []fruititem.FruitItem, requestID string, sequence uint64) (*middleware.Message, error) {
	body, err := serializeJSON(Envelope{
		Type:      messageType,
		Payload:   payload,
		RequestID: requestID,
		Sequence:  sequence,
	})
	if err != nil {
		return nil, err
	}
	message := middleware.Message{Body: string(body)}

	return &message, nil
}

func DeserializeMessage(message *middleware.Message) (*Envelope, error) {
	data, err := deserializeJSON([]byte(message.Body))
	if err != nil {
		return nil, err
	}

	if data.Type == "" {
		return nil, errors.New("message type is required")
	}

	if data.Payload == nil {
		data.Payload = []fruititem.FruitItem{}
	}

	return data, nil
}
