package outputprometheus

import (
	"context"
	"net/http"
	"regexp"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/goglog"
	"github.com/tsaikd/gogstash/config/logevent"
)

const (
	// ModuleName is the name used in config file
	ModuleName = "prometheus"
	// Counter is the counter metric type
	Counter = 0
	// Gauge is the gauge metric type
	Gauge = 1
)

// OutputConfig holds the configuration json fields and internal objects
type OutputConfig struct {
	config.OutputConfig
	Address    string                `json:"address,omitempty"`
	AppConfigs map[string]*AppConfig `json:"app_configs,omitempty"`
}

// AppConfig holds the configuration for each app
type AppConfig struct {
	Regex      string               `json:"regex,omitempty"`
	MetricName string               `json:"metric_name,omitempty"`
	MetricType int                  `json:"metric_type,omitempty"`
	MsgMetric  prometheus.Collector `json:"-"`
}

// DefaultOutputConfig returns an OutputConfig struct with default values
func DefaultOutputConfig() OutputConfig {
	// default app is gogstash
	appConfs := make(map[string]*AppConfig, 1)
	appConfs["gogstash"] = &AppConfig{
		MsgMetric: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "processed_messages_total",
		}),
	}

	return OutputConfig{
		OutputConfig: config.OutputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		Address: ":8080",

		AppConfigs: appConfs,
	}
}

// InitHandler initialize the output plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeOutputConfig, error) {
	conf := DefaultOutputConfig()
	if err := config.ReflectConfig(raw, &conf); err != nil {
		return nil, err
	}

	// register metric for each app
	for _, v := range conf.AppConfigs {
		if err := prometheus.Register(v.MsgMetric); err != nil {
			return nil, err
		}
	}

	go conf.serveHTTP()

	return &conf, nil
}

// Output event
func (o *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	msg := event.Message
	// filter by app name
	appName := event.GetString("log_topics")

	if v, ok := o.AppConfigs[appName]; ok {
		r := regexp.MustCompile(v.Regex)
		msgMetric := v.MsgMetric
		metricType := v.MetricType

		if r.MatchString(msg) {
			// for counter type metric
			if metricType == Counter {
				msgMetric.(prometheus.Counter).Inc()
			}

			// for gauge type metric
			if metricType == Gauge {
				// filter gauge number from message
				numStr := r.ReplaceAllLiteralString(msg, "")
				s, err := strconv.ParseFloat(numStr, 64)
				if err != nil {
					return err
				}

				msgMetric.(prometheus.Gauge).Set(s)
			}
		}
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
