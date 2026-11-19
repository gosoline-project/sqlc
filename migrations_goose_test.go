package sqlc

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/justtrackio/gosoline/pkg/cfg"
	"github.com/justtrackio/gosoline/pkg/clock"
	"github.com/justtrackio/gosoline/pkg/log"
)

func TestGooseLoggerPreservesPercentSigns(t *testing.T) {
	var output bytes.Buffer
	handler := log.NewHandlerIoWriter(cfg.New(), log.PriorityTrace, log.FormatterConsole, "test", "15:04:05", &output)
	logger := gooseLogger{ctx: context.Background(), logger: log.NewLoggerWithInterfaces(clock.NewRealClock(), []log.Handler{handler})}

	for _, test := range []struct {
		name string
		call func(...any)
	}{
		{name: "Fatal", call: logger.Fatal},
		{name: "Print", call: logger.Print},
		{name: "Println", call: logger.Println},
	} {
		t.Run(test.name, func(t *testing.T) {
			output.Reset()
			test.call("migration is 100% complete")
			if !strings.Contains(output.String(), "migration is 100% complete") {
				t.Fatalf("log message corrupted: %q", output.String())
			}
		})
	}
}
