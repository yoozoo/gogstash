package outputprometheus

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/tsaikd/gogstash/config"
	"github.com/tsaikd/gogstash/config/goglog"
	"github.com/tsaikd/gogstash/config/logevent"
	prometheus_conf "github.com/tsaikd/gogstash/config/prometheus"
	protoconf "github.com/yoozoo/protoconf_go"
)

const (
	// ModuleName is the name used in config file
	ModuleName = "prometheus"
	// counter is the counter metric type
	counter = 0
	// gauge is the gauge metric type
	gauge = 1
	// appNameField is the field for app name
	appNameField = "fields.log_topics"

	appToken = "U2FsdGVkX1/ABMEECkUiiZ6wKgfA3R5pDR7iOvwrBbhqkulGlZ1pDFX/9mVDCQiP"
)

var regexMap = make(map[string]*regexp.Regexp)

// OutputConfig holds the configuration json fields and internal objects
type OutputConfig struct {
	config.OutputConfig
	Address    string `json:"address,omitempty"`
	AppConfigs map[string]AppConfig
}

// AppConfig holds all the metrics information for app
type AppConfig struct {
	Metrics map[string]Metric
}

// Metric holds the metric configuration
type Metric struct {
	Regex      string               `json:"regex,omitempty"`
	MetricType int                  `json:"metric_type,omitempty"`
	Collector  prometheus.Collector `json:"-"`
}

func initConfig() (*OutputConfig, error) {
	// get etcd connection
	etcd := protoconf.NewEtcdReader("default")
	etcd.SetToken(appToken)
	reader := protoconf.NewConfigurationReader(etcd)
	// get config instance
	conf := prometheus_conf.GetInstance()
	if err := reader.Config(conf); err != nil {
		return nil, err
	}

	// watch app configs change
	reader.WatchKeys(conf)

	// save app config in the new map with the new app name
	newAppConfs := make(map[string]AppConfig)

	appConfigs := conf.GetApp_configs()

	for _, appConfig := range appConfigs {
		appName := appConfig.GetApp_name()
		newMetrics := make(map[string]Metric)

		for _, metric := range appConfig.GetMetrics() {
			metricType := int(metric.GetMetric_type())
			metricName := metric.GetMetric_name()

			// add new metric collector
			var collector prometheus.Collector
			// according to metric type
			switch metricType {
			case counter:
				collector = prometheus.NewCounter(prometheus.CounterOpts{
					Namespace: appName,
					Name:      metricName,
				})
			case gauge:
				collector = prometheus.NewGauge(prometheus.GaugeOpts{
					Namespace: appName,
					Name:      metricName,
				})
			default:
				return nil, fmt.Errorf("config init failed: unsupported metric type")
			}

			// register collector for each app
			if err := prometheus.Register(collector); err != nil {
				return nil, err
			}

			newMetrics[metricName] = Metric{
				Collector:  collector,
				MetricType: metricType,
				Regex:      metric.GetRegex(),
			}
		}

		newAppConfs[appName] = AppConfig{
			Metrics: newMetrics,
		}
	}

	return &OutputConfig{
		OutputConfig: config.OutputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		Address:    conf.GetAddress(),
		AppConfigs: newAppConfs,
	}, nil
}

// InitHandler initialize the output plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeOutputConfig, error) {
	// initialize config and register metrics
	conf, err := initConfig()
	if err != nil {
		return nil, err
	}

	go conf.serveHTTP()

	return conf, nil
}

// Output event
func (o *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	msg := event.Message
	// filter by app name
	appName := event.GetString(appNameField)

	if appConf, ok := o.AppConfigs[appName]; ok {
		// check each rule in the app config
		for metricName, metric := range appConf.Metrics {
			// find regex from memory
			r, ok := regexMap[appName+"_"+metricName]
			if !ok {
				// compile new regex if not found and put in map
				r = regexp.MustCompile(metric.Regex)
				regexMap[appName+"_"+metricName] = r
			}

			collector := metric.Collector
			metricType := metric.MetricType

			if r.MatchString(msg) {
				switch metricType {
				case counter:
					// for counter type metric
					collector.(prometheus.Counter).Inc()

				case gauge:
					// for gauge type metric
					// filter gauge number from message
					numStr := r.ReplaceAllLiteralString(msg, "")
					s, err := strconv.ParseFloat(numStr, 64)
					if err != nil {
						return err
					}

					collector.(prometheus.Gauge).Set(s)

				default:
					return fmt.Errorf("generate output failed: unsupported metric type")
				}
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
