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

	env = "default"
)

var regexMap = make(map[string]*regexp.Regexp)
var appMaps = make(map[string]*appCh)

// OutputConfig holds the configuration json fields and internal objects
type OutputConfig struct {
	config.OutputConfig
	Address string `json:"address,omitempty"`
}

type appCh struct {
	dataCh chan *logevent.LogEvent
	cfgCh  chan *appCfg
}

// appCfg holds all the metrics information for app
type appCfg struct {
	appName string
	metrics map[string]metric
}

// metric holds the metric configuration
type metric struct {
	regex      string
	metricType int
	collector  prometheus.Collector
}

func initConfig() (*OutputConfig, error) {
	// get etcd connection
	conf, err := getConfigFromProtoconf()
	if err != nil {
		return nil, err
	}

	for _, appConfig := range conf.GetApp_configs() {
		appName := appConfig.GetApp_name()
		// create new appCh and create new data channel and config channel
		appMaps[appName] = &appCh{
			dataCh: make(chan *logevent.LogEvent),
			cfgCh:  make(chan *appCfg, 1),
		}
		// add each metric configuration to the app configuration
		appConf := &appCfg{
			appName: appName,
			metrics: make(map[string]metric),
		}
		for _, metric := range appConfig.GetMetrics() {
			err = appConf.regNewMetric(metric)
			if err != nil {
				return nil, err
			}
		}
		// send app config to the config channel
		appMaps[appName].cfgCh <- appConf
	}

	outputConf := &OutputConfig{
		OutputConfig: config.OutputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		Address: conf.GetAddress(),
	}

	return outputConf, nil
}

func (a *appCfg) regNewMetric(m *prometheus_conf.Metric) error {
	metricType := int(m.GetMetric_type())
	metricName := m.GetMetric_name()

	// add new metric collector
	var collector prometheus.Collector
	// according to metric type
	switch metricType {
	case counter:
		collector = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: a.appName,
			Name:      metricName,
			Help:      "total " + metricName + " count of " + a.appName,
		})
	case gauge:
		collector = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: a.appName,
			Name:      metricName,
			Help:      metricName + " number of " + a.appName,
		})
	default:
		return fmt.Errorf("config init failed: unsupported metric type")
	}

	// register collector for each app
	if err := prometheus.Register(collector); err != nil {
		return err
	}

	a.metrics[metricName] = metric{
		collector:  collector,
		metricType: metricType,
		regex:      m.GetRegex(),
	}

	return nil
}

func getConfigFromProtoconf() (*prometheus_conf.Config, error) {
	// get etcd connection
	etcd := protoconf.NewEtcdReader(env)
	etcd.SetToken(appToken)
	reader := protoconf.NewConfigurationReader(etcd)
	// get config instance
	conf := prometheus_conf.GetInstance()
	if err := reader.Config(conf); err != nil {
		return nil, err
	}

	return conf, nil
}

// InitHandler initialize the output plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeOutputConfig, error) {
	// initialize config and register metrics
	conf, err := initConfig()
	if err != nil {
		return nil, err
	}

	// start go routine for each app
	for _, appCh := range appMaps {
		go processOutput(appCh.dataCh, appCh.cfgCh)
	}

	go conf.serveHTTP()

	return conf, nil
}

func processOutput(dataCh chan *logevent.LogEvent, cfgCh chan *appCfg) {
	// init app config
	var a *appCfg
	for {
		select {
		// new data arrives
		case data := <-dataCh:
			// check each rule in the app config
			for metricName, metric := range a.metrics {
				// find regex from memory
				r, ok := regexMap[a.appName+"_"+metricName]
				if !ok {
					// compile new regex if not found and put in map
					r = regexp.MustCompile(metric.regex)
					regexMap[a.appName+"_"+metricName] = r
				}

				collector := metric.collector
				metricType := metric.metricType

				msg := data.Message

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
							fmt.Printf("parse error: %s\n", err)
						}

						collector.(prometheus.Gauge).Set(s)

					default:
						fmt.Printf("generate output failed: unsupported metric type %d", metricType)
					}
				}
			}
		case cfg := <-cfgCh:
			// new config arrives
			a = cfg
		default:
		}
	}
}

// Output event
func (o *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	// filter by app name
	appName := event.GetString(appNameField)

	// find app in the app map of channels
	if appCh, ok := appMaps[appName]; ok {
		// send data to the data channel
		appCh.dataCh <- &event
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
