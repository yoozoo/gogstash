package outputprometheus

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

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
	// protoconf default config
	appToken    = "U2FsdGVkX1/ABMEECkUiiZ6wKgfA3R5pDR7iOvwrBbhqkulGlZ1pDFX/9mVDCQiP"
	env         = "default"
	reloadDelay = 10 * time.Second
)

var (
	appMap = make(map[string]*appCh)
	mutex  sync.RWMutex
)

// OutputConfig holds the configuration json fields and internal objects
type OutputConfig struct {
	config.OutputConfig
	Address string `json:"address,omitempty"`
}

type appCh struct {
	dataCh chan logevent.LogEvent
	cfgCh  chan appCfg
	quitCh chan bool
}

// appCfg holds all the metrics information for app
type appCfg struct {
	appName    string
	prodID     string
	prodName   string
	metricCfgs map[string]*metricCfg
}

// metricCfg holds the metric configuration
type metricCfg struct {
	metricName  string
	metricType  int32
	msgRegexStr string
	collector   prometheus.Collector
	filters     map[string]*filterCfg
	msgRegex    *regexp.Regexp
}

type filterCfg struct {
	field    string
	regexStr string
	regex    *regexp.Regexp
}

func initConfig() (*OutputConfig, error) {
	// load app config from protoconf
	etcd := protoconf.NewEtcdReader(env)
	etcd.SetToken(appToken)
	reader := protoconf.NewConfigurationReader(etcd)
	conf := prometheus_conf.GetInstance()
	if err := reader.Config(conf); err != nil {
		return nil, err
	}
	loadAppConfig()

	reloadCh := make(chan bool)
	go reloadCfg(reloadCh)

	// watch app configuration change
	conf.WatchApp_configs(func(string) { reloadCh <- true })
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

func reloadCfg(reloadCh chan bool) {
	isReload := false
	// check every 10 seconds
	ticker := time.NewTicker(reloadDelay)
	defer ticker.Stop()

	for {
		select {
		case <-reloadCh:
			// set flag
			isReload = true
		case <-ticker.C:
			// reload if flag is set
			if isReload {
				loadAppConfig()
				isReload = false
			}
		}
	}
}

func loadAppConfig() {
	conf := prometheus_conf.GetInstance()
	newApps := make(map[string]int)

	for k, appConfig := range conf.GetApp_configs() {
		appName := appConfig.GetApp_name()

		mutex.Lock()

		newCfg := appCfg{
			appName:    appName,
			prodID:     strings.Split(k, ".")[0],
			prodName:   strings.Split(k, ".")[1],
			metricCfgs: make(map[string]*metricCfg),
		}
		for _, metric := range appConfig.GetMetrics() {
			filterCfgs := make(map[string]*filterCfg)
			for _, filter := range metric.GetFilters() {
				filterCfgs[filter.GetField()] = &filterCfg{
					regexStr: filter.GetRegex(),
					field:    filter.GetField(),
				}
			}
			newCfg.metricCfgs[metric.GetMetric_name()] = &metricCfg{
				metricName:  metric.GetMetric_name(),
				metricType:  int32(metric.GetMetric_type()),
				msgRegexStr: metric.GetMessage_regex(),
				filters:     filterCfgs,
			}
		}

		if a, ok := appMap[appName]; ok {
			a.cfgCh <- newCfg
		} else {
			// new app config
			// send app config to the config channel
			// create new appCh and create new data channel and config channel
			a := &appCh{
				dataCh: make(chan logevent.LogEvent),
				cfgCh:  make(chan appCfg),
				quitCh: make(chan bool),
			}
			go processOutput(a.dataCh, a.cfgCh, a.quitCh)
			a.cfgCh <- newCfg
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

}

// InitHandler initialize the output plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeOutputConfig, error) {
	// initialize config
	conf, err := initConfig()
	if err != nil {
		return nil, err
	}

	go conf.serveHTTP()

	return conf, nil
}

func processOutput(dataCh chan logevent.LogEvent, cfgCh chan appCfg, quitCh chan bool) {
	// init app config
	a := &appCfg{
		metricCfgs: make(map[string]*metricCfg),
	}
	for {
		select {
		case cfg := <-cfgCh:
			newMetricMap := make(map[string]int)

			// when new config comes, check if it is the same, and if not, replace with the new config
			for _, metricCfg := range cfg.metricCfgs {
				metricName := metricCfg.metricName
				newMetricMap[metricName] = 1
				if m, ok := a.metricCfgs[metricName]; ok {
					if !isMetricEqual(m, metricCfg) {
						// metric not the same
						prometheus.Unregister(m.collector)
						err := a.regNewMetric(metricCfg)
						if err != nil {
							fmt.Printf("failed to register new metric: %s\n", err)
						}
					}
				} else {
					// add new metric
					a.appName = cfg.appName
					a.prodID = cfg.prodID
					a.prodName = cfg.prodName

					err := a.regNewMetric(metricCfg)
					if err != nil {
						fmt.Printf("failed to register new metric: %s\n", err)
					}
				}
			}

			// delete metrics not exist in runtime
			for _, metricCfg := range a.metricCfgs {
				if _, ok := newMetricMap[metricCfg.metricName]; !ok {
					prometheus.Unregister(metricCfg.collector)
					delete(a.metricCfgs, metricCfg.metricName)
				}
			}
		case data := <-dataCh:
			// new data comes
			// check each rule in the app config
			for _, m := range a.metricCfgs {
				collector := m.collector
				metricType := m.metricType

				isMatch := true
				for k, v := range m.filters {
					if v.regex == nil {
						fmt.Printf("filter %s check failed: regex nil", k)
						isMatch = false
						break
					} else if !v.regex.MatchString(data.GetString(k)) {
						isMatch = false
						break
					}
				}

				msg := data.Message
				if m.msgRegex != nil && isMatch {
					switch metricType {
					case counter:
						// for counter type metric
						if m.msgRegex.MatchString(msg) {
							collector.(prometheus.Counter).Inc()
						}

					case gauge:
						// for gauge type metric
						// filter gauge number from message
						match := m.msgRegex.FindStringSubmatch(msg)
						if len(match) >= 1 {
							s, err := strconv.ParseFloat(match[1], 64)
							if err != nil {
								fmt.Printf("parse error: %s\n", err)
							}

							collector.(prometheus.Gauge).Set(s)
						}

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

func isMetricEqual(old *metricCfg, new *metricCfg) bool {
	if old.metricType == new.metricType && old.msgRegexStr == new.msgRegexStr {
		if len(old.filters) != len(new.filters) {
			return false
		}

		for k, v := range new.filters {
			if f, ok := old.filters[k]; ok {
				if v.field != f.field || v.regexStr != f.regexStr {
					return false
				}
			} else {
				return false
			}
		}

		return true
	}

	return false
}

func (a *appCfg) regNewMetric(m *metricCfg) (err error) {
	metricType := m.metricType
	metricName := m.metricName

	// add new metric collector
	var collector prometheus.Collector
	// according to metric type
	switch metricType {
	case counter:
		collector = prometheus.NewCounter(prometheus.CounterOpts{
			Name:        metricName,
			Help:        "counter type: " + a.appName + "_" + metricName,
			ConstLabels: prometheus.Labels{"_product_id": a.prodID, "_product_name": a.prodName, "_name": a.appName},
		})
	case gauge:
		collector = prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        metricName,
			Help:        "gauge type:" + a.appName + "_" + metricName,
			ConstLabels: prometheus.Labels{"_product_id": a.prodID, "_product_name": a.prodName, "_name": a.appName},
		})
	default:
		return fmt.Errorf("config init failed: unsupported metric type")
	}

	// register collector for each app
	if err := prometheus.Register(collector); err != nil {
		return err
	}

	// compile regex
	for _, v := range m.filters {
		v.regex, err = regexp.Compile(v.regexStr)
		if err != nil {
			fmt.Printf("compile regex failed: %s\n", err)
		}
	}

	m.collector = collector
	m.msgRegex, err = regexp.Compile(m.msgRegexStr)
	if err != nil {
		return err
	}
	a.metricCfgs[metricName] = m

	return nil
}

// Output event
func (o *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	// filter by app name
	appName := event.GetString(appNameField)

	mutex.RLock()
	// find app in the app map of channels
	if appCh, ok := appMap[appName]; ok {
		// send data to the data channel
		appCh.dataCh <- event
	}
	mutex.RUnlock()

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
