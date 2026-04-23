package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	id               int
	inputQueue       middleware.Middleware
	controlConsumer  middleware.Middleware
	controlPublisher middleware.Middleware
	outputExchanges  []middleware.Middleware
	requestStates    map[string]map[string]fruititem.FruitItem
	stateMutex       sync.Mutex
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	controlConsumer, err := middleware.CreateExchangeMiddleware(
		fmt.Sprintf("%s_control", config.SumPrefix),
		[]string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)},
		connSettings,
	)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	controlRoutingKeys := make([]string, config.SumAmount)
	for i := range config.SumAmount {
		controlRoutingKeys[i] = fmt.Sprintf("%s_%d", config.SumPrefix, i)
	}

	controlPublisher, err := middleware.CreateExchangeMiddleware(
		fmt.Sprintf("%s_control", config.SumPrefix),
		controlRoutingKeys,
		connSettings,
	)
	if err != nil {
		inputQueue.Close()
		controlConsumer.Close()
		return nil, err
	}

	outputExchanges := make([]middleware.Middleware, config.AggregationAmount)
	for i := range config.AggregationAmount {
		routeKey := fmt.Sprintf("%s_%d", config.AggregationPrefix, i)

		outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, []string{routeKey}, connSettings)
		if err != nil {
			inputQueue.Close()
			controlConsumer.Close()
			controlPublisher.Close()
			for _, exchange := range outputExchanges {
				if exchange != nil {
					exchange.Close()
				}
			}
			return nil, err
		}
		outputExchanges[i] = outputExchange
	}

	return &Sum{
		id:               config.Id,
		inputQueue:       inputQueue,
		controlConsumer:  controlConsumer,
		controlPublisher: controlPublisher,
		outputExchanges:  outputExchanges,
		requestStates:    map[string]map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
	go sum.controlConsumer.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if envelope.Type == inner.TypeEOF {
		if err := sum.broadcastEOF(envelope.RequestID, envelope.Sequence); err != nil {
			slog.Error("While broadcasting eof", "err", err)
		}
		return
	}

	if envelope.Type == inner.TypeSumEOF {
		if err := sum.handleEndOfRecordMessage(envelope.RequestID, envelope.Sequence); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}
	if err := sum.handleDataMessage(envelope.RequestID, envelope.Payload); err != nil {
		slog.Error("While handling data message", "err", err)
		return
	}
}

func (sum *Sum) broadcastEOF(requestID string, sequence uint64) error {
	message, err := inner.SerializeMessage(inner.TypeSumEOF, []fruititem.FruitItem{}, requestID, sequence)
	if err != nil {
		return err
	}
	return sum.controlPublisher.Send(*message)
}

func (sum *Sum) handleEndOfRecordMessage(requestID string, eofSequence uint64) error {
	slog.Info("Received End Of Records message", "request_id", requestID, "sequence", eofSequence)

	sum.stateMutex.Lock()
	fruitItems := make([]fruititem.FruitItem, 0, len(sum.requestStates[requestID]))
	for _, fruitItem := range sum.requestStates[requestID] {
		fruitItems = append(fruitItems, fruitItem)
	}
	delete(sum.requestStates, requestID)
	sum.stateMutex.Unlock()

	for _, fruitItem := range fruitItems {
		fruitRecord := []fruititem.FruitItem{fruitItem}
		message, err := inner.SerializeMessage(inner.TypeData, fruitRecord, requestID, 0)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchanges[sum.aggregationIndex(requestID, fruitItem.Fruit)].Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	message, err := inner.SerializeMessageFrom(inner.TypeEOF, []fruititem.FruitItem{}, requestID, eofSequence, strconv.Itoa(sum.id))
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	for _, outputExchange := range sum.outputExchanges {
		if err := outputExchange.Send(*message); err != nil {
			slog.Debug("While sending EOF message", "err", err)
			return err
		}
	}
	return nil
}

// aggregationIndex returns the deterministic Aggregation partition for a fruit
// within a request. Including requestID keeps different clients independent.
func (sum *Sum) aggregationIndex(requestID string, fruit string) int {
	hasher := fnv.New32a()
	key := requestID + "|" + fruit
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32() % uint32(len(sum.outputExchanges)))
}

func (sum *Sum) handleDataMessage(requestID string, fruitRecords []fruititem.FruitItem) error {
	sum.stateMutex.Lock()
	if _, ok := sum.requestStates[requestID]; !ok {
		sum.requestStates[requestID] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitRecords {
		_, ok := sum.requestStates[requestID][fruitRecord.Fruit]
		if ok {
			sum.requestStates[requestID][fruitRecord.Fruit] = sum.requestStates[requestID][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.requestStates[requestID][fruitRecord.Fruit] = fruitRecord
		}
	}
	sum.stateMutex.Unlock()
	return nil
}
