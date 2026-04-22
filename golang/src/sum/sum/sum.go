package sum

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

// TODO: eliminar esto
const totalsLogFile = "sum_totals.log"

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
	inputQueue       middleware.Middleware
	controlConsumer  middleware.Middleware
	controlPublisher middleware.Middleware
	outputExchange   middleware.Middleware
	requestStates    map[string]map[string]fruititem.FruitItem
	pendingEOFs      map[string]uint64
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

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		controlConsumer.Close()
		controlPublisher.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:       inputQueue,
		controlConsumer:  controlConsumer,
		controlPublisher: controlPublisher,
		outputExchange:   outputExchange,
		requestStates:    map[string]map[string]fruititem.FruitItem{},
		pendingEOFs:      map[string]uint64{},
	}, nil
}

// TODO: validar si no es necesario que se espere o joinee esta go routine, como minimo al salir
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
		sum.registerPendingEOF(envelope.RequestID, envelope.Sequence)
		return
	}

	if err := sum.processPendingEOFsBefore(envelope.Sequence); err != nil {
		slog.Error("While processing pending eof messages", "err", err)
		return
	}

	// TODO: antes de cerrar definitivamente un request por EOF pendiente, hay que
	// incorporar el control fino de mensajes nacked/reintentados asociados a ese request.
	if err := sum.handleDataMessage(envelope.RequestID, envelope.Payload); err != nil {
		slog.Error("While handling data message", "err", err)
		return
	}
	if err := sum.logTotalsSnapshot("after data message"); err != nil {
		slog.Error("While logging totals snapshot", "err", err)
	}
}

func (sum *Sum) broadcastEOF(requestID string, sequence uint64) error {
	message, err := inner.SerializeMessage(inner.TypeSumEOF, []fruititem.FruitItem{}, requestID, sequence)
	if err != nil {
		return err
	}
	return sum.controlPublisher.Send(*message)
}

func (sum *Sum) registerPendingEOF(requestID string, sequence uint64) {
	if current, ok := sum.pendingEOFs[requestID]; ok && current >= sequence {
		return
	}
	sum.pendingEOFs[requestID] = sequence
}

func (sum *Sum) processPendingEOFsBefore(currentSequence uint64) error {
	for requestID, eofSequence := range sum.pendingEOFs {
		if eofSequence < currentSequence {
			if err := sum.handleEndOfRecordMessage(requestID, eofSequence); err != nil {
				return err
			}
			delete(sum.pendingEOFs, requestID)
		}
	}
	return nil
}

func (sum *Sum) handleEndOfRecordMessage(requestID string, eofSequence uint64) error {
	slog.Info("Received End Of Records message", "request_id", requestID, "sequence", eofSequence)

	fruitItemMap, ok := sum.requestStates[requestID]
	if !ok {
		return nil
	}

	for key := range fruitItemMap {
		fruitRecord := []fruititem.FruitItem{fruitItemMap[key]}
		message, err := inner.SerializeMessage(inner.TypeData, fruitRecord, requestID, 0)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	message, err := inner.SerializeMessage(inner.TypeEOF, []fruititem.FruitItem{}, requestID, eofSequence)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	delete(sum.requestStates, requestID)
	return nil
}

func (sum *Sum) handleDataMessage(requestID string, fruitRecords []fruititem.FruitItem) error {
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
	return nil
}

// TODO: eliminar esto, es solo para probar que los containers de sum esten
// realmente haciendo cosas
func (sum *Sum) logTotalsSnapshot(stage string) error {
	file, err := os.OpenFile(totalsLogFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer file.Close()

	var builder strings.Builder
	builder.WriteString(stage)
	builder.WriteString("\n")

	requestIDs := make([]string, 0, len(sum.requestStates))
	for requestID := range sum.requestStates {
		requestIDs = append(requestIDs, requestID)
	}
	sort.Strings(requestIDs)

	for _, requestID := range requestIDs {
		builder.WriteString(fmt.Sprintf("[%s]\n", requestID))
		keys := make([]string, 0, len(sum.requestStates[requestID]))
		for fruit := range sum.requestStates[requestID] {
			keys = append(keys, fruit)
		}
		sort.Strings(keys)
		for _, fruit := range keys {
			builder.WriteString(fmt.Sprintf("%s,%d\n", fruit, sum.requestStates[requestID][fruit].Amount))
		}
	}
	builder.WriteString("\n")

	_, err = file.WriteString(builder.String())
	return err
}
