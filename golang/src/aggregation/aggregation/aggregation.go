package aggregation

import (
	"fmt"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type AggregationConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Aggregation struct {
	outputQueue   middleware.Middleware
	inputExchange middleware.Middleware
	sumAmount     int
	requestStates map[string]map[string]fruititem.FruitItem
	eofCounts     map[string]int
	topSize       int
}

func NewAggregation(config AggregationConfig) (*Aggregation, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	inputExchangeRoutingKey := []string{fmt.Sprintf("%s_%d", config.AggregationPrefix, config.Id)}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, inputExchangeRoutingKey, connSettings)
	if err != nil {
		outputQueue.Close()
		return nil, err
	}

	return &Aggregation{
		outputQueue:   outputQueue,
		inputExchange: inputExchange,
		sumAmount:     config.SumAmount,
		requestStates: map[string]map[string]fruititem.FruitItem{},
		eofCounts:     map[string]int{},
		topSize:       config.TopSize,
	}, nil
}

func (aggregation *Aggregation) Run() {
	aggregation.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		aggregation.handleMessage(msg, ack, nack)
	})
}

func (aggregation *Aggregation) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	envelope, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if envelope.Type == inner.TypeEOF {
		if err := aggregation.handleEndOfRecordsMessage(envelope.RequestID, envelope.Sequence); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	aggregation.handleDataMessage(envelope.RequestID, envelope.Payload)
}

func (aggregation *Aggregation) handleEndOfRecordsMessage(requestID string, sequence uint64) error {
	slog.Info("Received End Of Records message", "request_id", requestID, "sequence", sequence)

	// TODO: reemplazar este contador por un set/lista de confirmaciones faltantes
	// por id o nombre de Sum. Si llega dos veces un EOF del mismo origen, el
	// contador actual puede cerrar el request antes de tiempo.
	aggregation.eofCounts[requestID]++
	if aggregation.eofCounts[requestID] < aggregation.sumAmount {
		return nil
	}

	fruitTopRecords := aggregation.buildFruitTop(requestID)
	message, err := inner.SerializeMessage(inner.TypeData, fruitTopRecords, requestID, sequence)
	if err != nil {
		slog.Debug("While serializing top message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending top message", "err", err)
		return err
	}

	message, err = inner.SerializeMessage(inner.TypeEOF, []fruititem.FruitItem{}, requestID, sequence)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := aggregation.outputQueue.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	delete(aggregation.requestStates, requestID)
	delete(aggregation.eofCounts, requestID)
	return nil
}

func (aggregation *Aggregation) handleDataMessage(requestID string, fruitRecords []fruititem.FruitItem) {
	if _, ok := aggregation.requestStates[requestID]; !ok {
		aggregation.requestStates[requestID] = map[string]fruititem.FruitItem{}
	}

	for _, fruitRecord := range fruitRecords {
		if _, ok := aggregation.requestStates[requestID][fruitRecord.Fruit]; ok {
			aggregation.requestStates[requestID][fruitRecord.Fruit] = aggregation.requestStates[requestID][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			aggregation.requestStates[requestID][fruitRecord.Fruit] = fruitRecord
		}
	}
}

func (aggregation *Aggregation) buildFruitTop(requestID string) []fruititem.FruitItem {
	fruitItemMap, ok := aggregation.requestStates[requestID]
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
	finalTopSize := min(aggregation.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}
