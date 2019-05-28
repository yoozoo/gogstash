package outputprometheus

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"sync"

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

var (
	regexMap = make(map[string]*regexp.Regexp)
	appMap   = make(map[string]*appCh)
	mutex    sync.RWMutex
	reader   *protoconf.ConfigurationReader
)

// OutputConfig holds the configuration json fields and internal objects
type OutputConfig struct {
	config.OutputConfig
	Address string `json:"address,omitempty"`
}

type appCh struct {
	dataCh chan logevent.LogEvent
	cfgCh  chan *prometheus_conf.AppConfig
	quitCh chan bool
}

// appCfg holds all the metrics information for app
type appCfg struct {
	appName    string
	metricCfgs map[string]*metricCfg
}

// metricCfg holds the metric configuration
type metricCfg struct {
	metric    prometheus_conf.Metric
	collector prometheus.Collector
	regex     regexp.Regexp
}

func initConfig() (*OutputConfig, error) {
	// load app config from protoconf
	conf, err := loadAppConfig()
	if err != nil {
		return nil, err
	}

	// watch app configuration change
	conf.WatchApp_configs(func(string) { loadAppConfig() })
	reader.WatchKeys(conf)

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

func loadAppConfig() (*prometheus_conf.Config, error) {
	// load from protoconf
	conf := prometheus_conf.GetInstance()
	if err := reader.Config(conf); err != nil {
		return nil, err
	}

	newApps := make(map[string]int)

	for _, appConfig := range conf.GetApp_configs() {
		appName := appConfig.GetApp_name()

		mutex.Lock()
		if a, ok := appMap[appName]; ok {
			a.cfgCh <- appConfig
		} else {
			// new app config
			// send app config to the config channel
			// create new appCh and create new data channel and config channel
			a := &appCh{
				dataCh: make(chan logevent.LogEvent),
				cfgCh:  make(chan *prometheus_conf.AppConfig, 1),
				quitCh: make(chan bool),
			}
			go processOutput(a.dataCh, a.cfgCh, a.quitCh)
			a.cfgCh <- appConfig
			appMap[appName] = a
		}
		mutex.Unlock()
		newApps[appName] = 1
	}

	// check if app config not exist in runtime, delete it
	for appName, a := range appMap {
		mutex.Lock()
		if _, ok := newApps[appName]; !ok {
			a.quitCh <- true
			delete(appMap, appName)
		}
		mutex.Unlock()
	}

	return conf, nil
}

// InitHandler initialize the output plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeOutputConfig, error) {
	// get etcd connection
	etcd := protoconf.NewEtcdReader(env)
	etcd.SetToken(appToken)
	reader = protoconf.NewConfigurationReader(etcd)

	// initialize config
	conf, err := initConfig()
	if err != nil {
		return nil, err
	}

	go conf.serveHTTP()

	return conf, nil
}

func processOutput(dataCh chan logevent.LogEvent, cfgCh chan *prometheus_conf.AppConfig, quitCh chan bool) {
	// init app config
	a := &appCfg{
		metricCfgs: make(map[string]*metricCfg),
	}
	for {
		select {
		case cfg := <-cfgCh:
			newMetricMap := make(map[string]int)

			// when new config comes, check if it is the same, and if not, replace with the new config
			for _, metric := range cfg.GetMetrics() {
				newMetricMap[metric.GetMetric_name()] = 1
				if m, ok := a.metricCfgs[metric.GetMetric_name()]; ok {
					if m.metric.GetMetric_type() != metric.GetMetric_type() || m.metric.GetRegex() != metric.GetRegex() {
						prometheus.Unregister(m.collector)
						err := a.regNewMetric(metric)
						if err != nil {
							fmt.Printf("failed to register new metric: %s\n", err)
						}

					}
				} else {
					// add new metric
					a.appName = cfg.GetApp_name()
					err := a.regNewMetric(metric)
					if err != nil {
						fmt.Printf("failed to register new metric: %s\n", err)
					}
				}
			}

			// delete metrics not exist in runtime
			for _, metricCfg := range a.metricCfgs {
				if _, ok := newMetricMap[metricCfg.metric.GetMetric_name()]; !ok {
					prometheus.Unregister(metricCfg.collector)
					delete(a.metricCfgs, metricCfg.metric.GetMetric_name())
				}
			}
		case data := <-dataCh:
			// new data comes
			// check each rule in the app config
			for _, m := range a.metricCfgs {
				collector := m.collector
				metricType := m.metric.GetMetric_type()

				msg := data.Message
				r := &m.regex

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
		case <-quitCh:
			// quit goroutine and unregister all metrics when app is deleted
			for _, m := range a.metricCfgs {
				prometheus.Unregister(m.collector)
			}
			return
		default:
		}
	}
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
			Help:      "counter type: " + a.appName + "_" + metricName,
		})
	case gauge:
		collector = prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: a.appName,
			Name:      metricName,
			Help:      "gauge type:" + a.appName + "_" + metricName,
		})
	default:
		return fmt.Errorf("config init failed: unsupported metric type")
	}

	// register collector for each app
	if err := prometheus.Register(collector); err != nil {
		return err
	}

	r := regexp.MustCompile(m.GetRegex())

	a.metricCfgs[metricName] = &metricCfg{
		collector: collector,
		metric:    *m,
		regex:     *r,
	}

	return nil
}

// Output event
func (o *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	// filter by app name
	appName := event.GetString(appNameField)

	// find app in the app map of channels
	if appCh, ok := appMap[appName]; ok {
		// send data to the data channel
		appCh.dataCh <- event
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
