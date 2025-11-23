package sqlc

import (
	"fmt"

	"github.com/justtrackio/gosoline/pkg/log"
)

type DriverFactory func(logger log.Logger) (Driver, error)

type Driver interface {
	GetDSN(settings *Settings) string
	GetPlaceholder() string
	GetQuote() string
}

var driverFactories = map[string]DriverFactory{}

func AddDriverFactory(name string, factory DriverFactory) {
	driverFactories[name] = factory
}

func GetDriver(logger log.Logger, driverName string) (Driver, error) {
	var ok bool
	var err error
	var factory DriverFactory
	var driver Driver

	if factory, ok = driverFactories[driverName]; !ok {
		return nil, fmt.Errorf("no driver factory defined for %s", driverName)
	}

	if driver, err = factory(logger); err != nil {
		return nil, fmt.Errorf("failed to get driver for %s: %w", driverName, err)
	}

	return driver, nil
}
