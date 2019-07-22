package outputprometheus

import (
	"context"
	"fmt"
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
	// appNameField is the field for app name
	appNameField = "fields.log_topics"
	// protoconf default config
	appToken    = "U2FsdGVkX1/ABMEECkUiiZ6wKgfA3R5pDR7iOvwrBbhqkulGlZ1pDFX/9mVDCQiP"
	env         = "default"
	reloadDelay = 10 * time.Second
)

var (
	appLoader atomic.Value

	counterRegistry = prometheus.NewRegistry()
	gaugeRegistry   = prometheus.NewRegistry()

	// used to lock the config map
	reloadMutex sync.Mutex
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
	metricName  string
	metricType  int32
	msgRegexStr string
	metric      prometheus.Collector
	filters     map[string]*filterCfg
	msgRegex    *regexp.Regexp
}

type filterCfg struct {
	field    string
	regexStr string
	regex    *regexp.Regexp
}

func initConfig() (*OutputConfig, error) {

	// initial with empty apps
	appLoader.Store(make(map[string]*appCh))

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
	// initialize config
	conf, err := initConfig()
	if err != nil {
		return nil, err
	}

	go conf.serveHTTP()

	return conf, nil
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
							fmt.Println("modify metric", metricCfg.metricName)
							deleteMetric(metricCfg)
							err := registerMetric(newMetricCfg, label)
							if err != nil {
								fmt.Printf("failed to register new metric: %s\n", err)
							}
						} else {
							newCfg.metricCfgs[metricCfg.metricName] = metricCfg
						}
					} else {
						// add new metric
						fmt.Println("add metric", newMetricCfg.metricName)
						err := registerMetric(newMetricCfg, label)
						if err != nil {
							fmt.Printf("failed to register new metric: %s\n", err)
						}
					}
				}

				// delete metrics not exist in runtime
				for metricName, metricCfg := range cfg.metricCfgs {
					if _, ok := newCfg.metricCfgs[metricName]; !ok {
						fmt.Println("delete old metric", metricName)
						deleteMetric(metricCfg)
					}
				}
			} else {
				if cfg != nil {
					fmt.Println("delete all old metric", cfg.appName)
					cfg.deleteAllMetrics()
				}
				fmt.Println("add all new metric", newCfg.appName)
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
					switch m.metricType {
					case counter:
						// for counter type metric
						if m.msgRegex.MatchString(msg) {
							m.metric.(prometheus.Counter).Inc()
						}
					case gauge:
						// for gauge type metric
						// filter gauge number from message
						match := m.msgRegex.FindStringSubmatch(msg)
						if len(match) >= 2 {
							s, err := strconv.ParseFloat(match[1], 64)
							if err != nil {
								fmt.Printf("parse error: %s\n", err)
							} else {
								m.metric.(prometheus.Gauge).Set(s)
							}
						}
					default:
						fmt.Printf("generate output failed: unsupported metric type %d", m.metricType)
					}
				}
			}
		case <-quitCh:
			// quit goroutine and unregister all metrics when app is deleted
			if cfg != nil {
				fmt.Println("delete before quit")
				cfg.deleteAllMetrics()
			}
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

func deleteMetric(m *metricCfg) {
	if m.metric != nil {
		fmt.Println("delete metric ", m.metricName)
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
	fmt.Println("register for ", m.metricName, "with label", label)
	// compile regex
	for _, v := range m.filters {
		v.regex, err = regexp.Compile(v.regexStr)
		if err != nil {
			fmt.Println("compile regex", v.regexStr, "failed:", err)
			return err
		}
	}

	m.msgRegex, err = regexp.Compile(m.msgRegexStr)
	if err != nil {
		fmt.Println("compile regex", m.msgRegexStr, "failed:", err)
		return err
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
		}
		return err
	case gauge:
		metric := prometheus.NewGauge(prometheus.GaugeOpts{
			Name:        m.metricName,
			Help:        "default help",
			ConstLabels: label,
		})
		err := gaugeRegistry.Register(metric)
		if err == nil {
			m.metric = metric
		}
		return err
	default:
		return fmt.Errorf("config init failed: unsupported metric type")
	}
}

// Output event
func (o *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	// filter by app name
	appName := event.GetString(appNameField)
	if len(appName) > 0 {
		if apps, ok := appLoader.Load().(map[string]appCh); ok {
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
