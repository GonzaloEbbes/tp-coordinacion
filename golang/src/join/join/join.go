package join

import (
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue           middleware.Middleware
	outputQueue          middleware.Middleware
	aggregationAmount    int
	requestStates        map[string]map[string]fruititem.FruitItem
	partialConfirmations map[string]map[string]struct{}
	topSize              int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue:           inputQueue,
		outputQueue:          outputQueue,
		aggregationAmount:    config.AggregationAmount,
		requestStates:        map[string]map[string]fruititem.FruitItem{},
		partialConfirmations: map[string]map[string]struct{}{},
		topSize:              config.TopSize,
	}, nil
}

func (join *Join) Run() {
	join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing join message", "err", err)
		return
	}
	if envelope.Type != inner.TypeData {
		return
	}

	if err := join.handlePartialTop(envelope.RequestID, envelope.Payload, envelope.Sequence, envelope.Origin); err != nil {
		slog.Error("While handling partial top", "err", err)
	}
}

func (join *Join) handlePartialTop(requestID string, fruitRecords []fruititem.FruitItem, sequence uint64, origin string) error {
	if origin == "" {
		slog.Error("Ignoring partial top without origin", "request_id", requestID)
		return nil
	}

	if _, ok := join.partialConfirmations[requestID]; !ok {
		join.partialConfirmations[requestID] = map[string]struct{}{}
	}
	if _, duplicated := join.partialConfirmations[requestID][origin]; duplicated {
		return nil
	}
	join.partialConfirmations[requestID][origin] = struct{}{}

	if _, ok := join.requestStates[requestID]; !ok {
		join.requestStates[requestID] = map[string]fruititem.FruitItem{}
	}

	requestState := join.requestStates[requestID]
	for _, fruitRecord := range fruitRecords {
		if current, ok := requestState[fruitRecord.Fruit]; ok {
			requestState[fruitRecord.Fruit] = current.Sum(fruitRecord)
		} else {
			requestState[fruitRecord.Fruit] = fruitRecord
		}
	}

	if len(join.partialConfirmations[requestID]) < join.aggregationAmount {
		return nil
	}

	fruitTopRecords := join.buildFruitTop(requestID)
	message, err := inner.SerializeMessage(inner.TypeData, fruitTopRecords, requestID, sequence)
	if err != nil {
		return err
	}
	if err := join.outputQueue.Send(*message); err != nil {
		return err
	}

	delete(join.requestStates, requestID)
	delete(join.partialConfirmations, requestID)
	return nil
}

func (join *Join) buildFruitTop(requestID string) []fruititem.FruitItem {
	fruitItemMap, ok := join.requestStates[requestID]
	if !ok {
		return []fruititem.FruitItem{}
	}

	fruitItems := make([]fruititem.FruitItem, 0, len(fruitItemMap))
	for _, item := range fruitItemMap {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
