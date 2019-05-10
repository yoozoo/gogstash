package outputprometheus

import (
	"context"
	"net/http"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/goglog"
	"github.com/tsaikd/gogstash/config/logevent"
)

// ModuleName is the name used in config file
const ModuleName = "prometheus"

// OutputConfig holds the configuration json fields and internal objects
type OutputConfig struct {
	config.OutputConfig
	Address string `json:"address,omitempty"`

	MsgCount prometheus.Counter `json:"-"`
}

// DefaultOutputConfig returns an OutputConfig struct with default values
func DefaultOutputConfig() OutputConfig {
	return OutputConfig{
		OutputConfig: config.OutputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		Address: ":8080",
		MsgCount: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "status_400_total",
			Help: "Number of status code 400 in messages",
		}),
	}
}

// InitHandler initialize the output plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeOutputConfig, error) {
	conf := DefaultOutputConfig()
	if err := config.ReflectConfig(raw, &conf); err != nil {
		return nil, err
	}

	if err := prometheus.Register(conf.MsgCount); err != nil {
		return nil, err
	}
	go conf.serveHTTP()

	return &conf, nil
}

// Output event
func (o *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	if strings.Contains(event.Message, "\"status\":400") {
		o.MsgCount.Inc()
	}
	return
}

func (o *OutputConfig) serveHTTP() {
	logger := goglog.Logger
	http.Handle("/metrics", prometheus.Handler())
	logger.Infof("Listen %s", o.Address)
	if err := http.ListenAndServe(o.Address, nil); err != nil {
		logger.Fatal(err)
	}
}
