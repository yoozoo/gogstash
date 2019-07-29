package outputprometheus

import (
	"context"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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
	// protoconf default config
	env         = "default"
	reloadDelay = 10 * time.Second
)

var (
	appLoader atomic.Value

	counterRegistry = prometheus.NewRegistry()
	gaugeRegistry   = prometheus.NewRegistry()

	// used to lock the config map
	reloadMutex sync.Mutex

	appToken string

	// appNameField is the field for app name
	appNameField = "fields.log_topics"
)

// OutputConfig holds the configuration json fields and internal objects
type OutputConfig struct {
	config.OutputConfig
	Address string `json:"address,omitempty"`
}

type appCh struct {
	dataCh chan logevent.LogEvent
	cfgCh  chan *appCfg
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
	metricName string
	metricType int32
	metric     prometheus.Collector
	filters    []*filterCfg
}

type filterCfg struct {
	field    string
	regexStr string
	regex    *regexp.Regexp
}

func initConfig() (*OutputConfig, error) {

	// initial with empty apps
	appLoader.Store(make(map[string]*appCh))

	if len(strings.TrimSpace(appToken)) == 0 {
		goglog.Logger.Fatalln("Empty app token. Plsase set in output.type.token")
	}
	// load app config from protoconf
	etcd := protoconf.NewEtcdReader(env)
	etcd.SetToken(appToken)
	reader := protoconf.NewConfigurationReader(etcd)
	conf := prometheus_conf.GetInstance()
	if err := reader.Config(conf); err != nil {
		return nil, err
	}
	loadAppConfig()

	reloadCh := make(chan bool, 20)
	go reloadCfg(reloadCh)

	// watch app configuration change
	conf.WatchApp_configs(func(string) {
		reloadMutex.Lock()
		reloadCh <- true
		reloadMutex.Unlock()
	})

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
	// the apps tp create from configuration. once create, no modify to it.
	newApps := make(map[string]*appCh)
	// load current apps
	oldApps := appLoader.Load().(map[string]*appCh)

	// no update to app settings during loading
	reloadMutex.Lock()

	for k, appConfig := range conf.GetApp_configs() {
		if len(appConfig.GetMetrics()) == 0 {
			continue
		}

		appName := appConfig.GetApp_name()

		newCfg := appCfg{
			appName:    appName,
			prodID:     strings.Split(k, ".")[0],
			prodName:   strings.Split(k, ".")[1],
			metricCfgs: make(map[string]*metricCfg),
		}
		for _, metric := range appConfig.GetMetrics() {
			var filterCfgs []*filterCfg

			filters := metric.GetFilters()
			for i := 0; i < len(filters); i++ {
				if filter, ok := filters[strconv.Itoa(i)]; ok {
					filterCfgs = append(filterCfgs, &filterCfg{
						regexStr: filter.GetRegex(),
						field:    filter.GetField(),
					})
				} else {
					goglog.Logger.Errorln(k, metric.GetMetric_name(), "missing filter:", i)
				}
			}
			newCfg.metricCfgs[metric.GetMetric_name()] = &metricCfg{
				metricName: metric.GetMetric_name(),
				metricType: int32(metric.GetMetric_type()),
				filters:    filterCfgs,
			}
		}
		goglog.Logger.Debug("app:", appName, " cfg:", newCfg)
		if a, ok := oldApps[appName]; ok {
			// copy existing app and send the cfg
			newApps[appName] = a
			a.cfgCh <- &newCfg
		} else {
			// create new app and send the cfg
			a := &appCh{
				dataCh: make(chan logevent.LogEvent, 1000),
				cfgCh:  make(chan *appCfg, 1),
				quitCh: make(chan bool),
			}
			go processOutput(a.dataCh, a.cfgCh, a.quitCh)
			newApps[appName] = a
			a.cfgCh <- &newCfg
		}
	}
	reloadMutex.Unlock()
	// store new apps as current
	appLoader.Store(newApps)

	// check if app config not exist in runtime, send quit signal
	// notice here no delete from the map
	for appName, a := range oldApps {
		if _, ok := newApps[appName]; !ok {
			a.quitCh <- true
		}
	}
}

// InitHandler initialize the output plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeOutputConfig, error) {

	if token, ok := (*raw)["token"]; ok {
		if token, ok := token.(string); ok {
			appToken = token
		}
	}

	if appField, ok := (*raw)["appField"]; ok {
		if appField, ok := appField.(string); ok {
			appNameField = appField
		}
	}

	// initialize config
	conf, err := initConfig()
	if err != nil {
		return nil, err
	}

	go conf.serveHTTP()

	return conf, nil
}

func getFieldValue(data *logevent.LogEvent, field string) (str string, ok bool) {
	if field != "message" {
		if v, exist := data.GetValue(field); exist {
			switch tmp := v.(type) {
			case string:
				return tmp, true
			case float64:
				return strconv.FormatFloat(tmp, 'f', -1, 64), true
			case bool:
				return strconv.FormatBool(tmp), true
			default:
				return
			}
		}
		return
	}
	// return data.message
	return data.Message, true
}

func processCounter(data *logevent.LogEvent, m *metricCfg) {
	for _, filter := range m.filters {
		if str, ok := getFieldValue(data, filter.field); !ok {
			goglog.Logger.Debug("failed to get field:", filter.field, " from event:", data)
			return
		} else if filter.regex != nil && !filter.regex.MatchString(str) {
			goglog.Logger.Debug("failed to match regex:", filter.regexStr, " with string:", str)
			return
		}
	}
	m.metric.(prometheus.Counter).Inc()
}
func processGauge(data *logevent.LogEvent, m *metricCfg) {
	filterSize := len(m.filters)
	for i := 0; i < filterSize-1; i++ {
		filter := m.filters[i]
		if str, ok := getFieldValue(data, filter.field); !ok {
			goglog.Logger.Debug("failed to get field:", filter.field, " from event:", data)
			return
		} else if filter.regex != nil && !filter.regex.MatchString(str) {
			goglog.Logger.Debug("failed to match regex:", filter.regexStr, " with string:", str)
			return
		}
	}
	filter := m.filters[filterSize-1]
	if str, ok := getFieldValue(data, filter.field); ok {
		match := filter.regex.FindStringSubmatch(str)
		if len(match) >= 2 {
			s, err := strconv.ParseFloat(match[1], 64)
			if err != nil {
				goglog.Logger.Errorf("parse error: %s\n", err)
			} else {
				m.metric.(prometheus.Gauge).Set(s)
			}
		} else {
			goglog.Logger.Debug("failed to match regex:", filter.regexStr, " with string:", str)
		}
	} else {
		goglog.Logger.Debug("failed to get field:", filter.field, " from event:", data)
	}
}

func processOutput(dataCh chan logevent.LogEvent, cfgCh chan *appCfg, quitCh chan bool) {
	// init app config
	var cfg *appCfg
	for {
		select {
		case newCfg := <-cfgCh:

			if cfg != nil && newCfg.prodID == cfg.prodID && newCfg.prodName == cfg.prodName && newCfg.appName == cfg.appName {
				label := newCfg.getLabel()

				for _, newMetricCfg := range newCfg.metricCfgs {
					if metricCfg, ok := cfg.metricCfgs[newMetricCfg.metricName]; ok {
						if !isMetricEqual(metricCfg, newMetricCfg) {
							// metric not the same
							goglog.Logger.Debugln("modify metric", metricCfg.metricName)
							deleteMetric(metricCfg)
							err := registerMetric(newMetricCfg, label)
							if err != nil {
								goglog.Logger.Errorf("failed to register new metric: %s\n", err)
							}
						} else {
							newCfg.metricCfgs[metricCfg.metricName] = metricCfg
						}
					} else {
						// add new metric
						goglog.Logger.Debugln("add metric", newMetricCfg.metricName)
						err := registerMetric(newMetricCfg, label)
						if err != nil {
							goglog.Logger.Errorf("failed to register new metric: %s\n", err)
						}
					}
				}

				// delete metrics not exist in runtime
				for metricName, metricCfg := range cfg.metricCfgs {
					if _, ok := newCfg.metricCfgs[metricName]; !ok {
						goglog.Logger.Debugln("delete old metric:", metricName)
						deleteMetric(metricCfg)
					}
				}
			} else {
				if cfg != nil {
					goglog.Logger.Debugln("delete all old metric:", cfg.appName)
					cfg.deleteAllMetrics()
				}
				goglog.Logger.Debugln("add all new metric:", newCfg.appName)
				newCfg.registerAllMetrics()
			}
			cfg = newCfg
		case data := <-dataCh:
			// new data comes
			// check each rule in the app config
			if cfg == nil {
				continue
			}
			for _, m := range cfg.metricCfgs {
				if m.metric == nil {
					continue
				}
				switch m.metricType {
				case counter:
					processCounter(&data, m)
				case gauge:
					processGauge(&data, m)
				default:
					goglog.Logger.Errorf("generate output failed: unsupported metric type %d", m.metricType)
				}

			}
		case <-quitCh:
			// quit goroutine and unregister all metrics when app is deleted
			if cfg != nil {
				goglog.Logger.Debugln("delete before quit")
				cfg.deleteAllMetrics()
			}
		}
	}
}

func isMetricEqual(old *metricCfg, new *metricCfg) bool {
	if old.metricType != new.metricType || len(old.filters) != len(new.filters) {
		return false
	}

	for k, filter1 := range new.filters {
		filter2 := old.filters[k]
		if filter1.field != filter2.field || filter1.regexStr != filter2.regexStr {
			return false
		}
	}

	return true
}

func deleteMetric(m *metricCfg) {
	if m.metric != nil {
		goglog.Logger.Debugln("delete metric ", m.metricName)
		switch m.metricType {
		case counter:
			counterRegistry.Unregister(m.metric)
		case gauge:
			gaugeRegistry.Unregister(m.metric)
		}
	}
}

func (a *appCfg) getLabel() map[string]string {
	return map[string]string{"_product_id": a.prodID, "_product_name": a.prodName, "_name": a.appName}
}

func (a *appCfg) deleteAllMetrics() {
	for _, m := range a.metricCfgs {
		deleteMetric(m)
	}
}

func (a *appCfg) registerAllMetrics() {
	label := a.getLabel()
	for _, m := range a.metricCfgs {
		registerMetric(m, label)
	}
}
func registerMetric(m *metricCfg, label map[string]string) (err error) {

	if len(m.filters) == 0 && m.metricType != counter {
		return errors.New("non counter metric must have at least one filter")
	}
	// compile regex
	for _, v := range m.filters {
		if len(v.regexStr) != 0 {
			regex, err := regexp.Compile(v.regexStr)
			if err != nil {
				goglog.Logger.Errorln("compile regex", v.regexStr, "failed:", err)
				return err
			}
			v.regex = regex
		}
	}

	// register new metric if not exists
	switch m.metricType {
	case counter:
		metric := prometheus.NewCounter(prometheus.CounterOpts{
			Name:        m.metricName,
			Help:        "default help",
			ConstLabels: label,
		})
		err := counterRegistry.Register(metric)
		if err == nil {
			m.metric = metric
			goglog.Logger.Debug("new metric:", m)
			for _, filter := range m.filters {
				goglog.Logger.Debug("     filter:", filter)
			}
		}

		return err
	case gauge:
		filter := m.filters[len(m.filters)-1]
		if filter.regex == nil || filter.regex.NumSubexp() < 1 {
			goglog.Logger.Errorln("Gauge: last filter must have a capture")
			return errors.New("Gauge: last filter must have a capture")
		}
		metric := prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        m.metricName,
			Help:        "default help",
			ConstLabels: label,
		})
		err := gaugeRegistry.Register(metric)
		if err == nil {
			m.metric = metric
			goglog.Logger.Debug("new metric:", m)
			for _, filter := range m.filters {
				goglog.Logger.Debug("     filter:", filter)
			}
		}

		return err
	default:
		return errors.New("config init failed: unsupported metric type")
	}
}

// Output event
func (o *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	// filter by app name
	appName := event.GetString(appNameField)
	if len(appName) > 0 {
		if apps, ok := appLoader.Load().(map[string]*appCh); ok {
			if app, ok := apps[appName]; ok {
				app.dataCh <- event
			}
		}
	}
	return
}

func (o *OutputConfig) serveHTTP() {
	logger := goglog.Logger
	http.Handle("/counter", promhttp.HandlerFor(counterRegistry, promhttp.HandlerOpts{}))
	http.Handle("/gauge", promhttp.HandlerFor(gaugeRegistry, promhttp.HandlerOpts{}))
	logger.Infof("Listen %s", o.Address)
	if err := http.ListenAndServe(o.Address, nil); err != nil {
		logger.Fatal(err)
	}
}
