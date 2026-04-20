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
	inputQueue     middleware.Middleware
	outputExchange middleware.Middleware
	fruitItemMap   map[string]fruititem.FruitItem
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		fruitItemMap:   map[string]fruititem.FruitItem{},
	}, nil
}

func (sum *Sum) Run() {
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
		if err := sum.handleEndOfRecordMessage(); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		if err := sum.logTotalsSnapshot("after eof"); err != nil {
			slog.Error("While logging totals snapshot", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(envelope.Payload); err != nil {
		slog.Error("While handling data message", "err", err)
		return
	}
	if err := sum.logTotalsSnapshot("after data message"); err != nil {
		slog.Error("While logging totals snapshot", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage() error {
	slog.Info("Received End Of Records message")
	for key := range sum.fruitItemMap {
		fruitRecord := []fruititem.FruitItem{sum.fruitItemMap[key]}
		message, err := inner.SerializeMessage(inner.TypeData, fruitRecord, "")
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		if err := sum.outputExchange.Send(*message); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	message, err := inner.SerializeMessage(inner.TypeEOF, []fruititem.FruitItem{}, "")
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}
	return nil
}

func (sum *Sum) handleDataMessage(fruitRecords []fruititem.FruitItem) error {
	for _, fruitRecord := range fruitRecords {
		_, ok := sum.fruitItemMap[fruitRecord.Fruit]
		if ok {
			sum.fruitItemMap[fruitRecord.Fruit] = sum.fruitItemMap[fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.fruitItemMap[fruitRecord.Fruit] = fruitRecord
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

	keys := make([]string, 0, len(sum.fruitItemMap))
	for fruit := range sum.fruitItemMap {
		keys = append(keys, fruit)
	}
	sort.Strings(keys)

	var builder strings.Builder
	builder.WriteString(stage)
	builder.WriteString("\n")
	for _, fruit := range keys {
		builder.WriteString(fmt.Sprintf("%s,%d\n", fruit, sum.fruitItemMap[fruit].Amount))
	}
	builder.WriteString("\n")

	_, err = file.WriteString(builder.String())
	return err
}
