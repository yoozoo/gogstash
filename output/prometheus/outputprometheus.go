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
	Address    string            `json:"address,omitempty"`
	AppConfigs map[string][]Rule `json:"app_configs,omitempty"`
}

// Rule holds the configuration for each app
type Rule struct {
	Name       string               `json:"name,omitempty"`
	Regex      string               `json:"regex,omitempty"`
	MetricName string               `json:"metric_name,omitempty"`
	MetricType int                  `json:"metric_type,omitempty"`
	MsgMetric  prometheus.Collector `json:"-"`
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
	// generate output config
	address := conf.GetAddress()
	ruleMap := conf.GetApp_configs()

	appConfs := make(map[string][]Rule)

	for ruleName, rule := range ruleMap {
		metricName := rule.GetMetric_name()
		metricType := int(rule.GetMetric_type())

		// add new rule
		newRule := Rule{
			Name:       ruleName,
			Regex:      rule.GetRegex(),
			MetricName: metricName,
			MetricType: metricType,
		}

		// add new metric
		var metric prometheus.Collector
		// according to metric type
		switch metricType {
		case counter:
			metric = prometheus.NewCounter(prometheus.CounterOpts{
				Name: metricName,
			})
		case gauge:
			metric = prometheus.NewGauge(prometheus.GaugeOpts{
				Name: metricName,
			})
		default:
			return nil, fmt.Errorf("config init failed: unsupported metric type")
		}
		newRule.MsgMetric = metric

		// add rule to app config
		appName := rule.GetApp_name()

		appRules, ok := appConfs[appName]
		if ok {
			// if already add app
			appRules = append(appRules, newRule)
		} else {
			appRules = []Rule{
				newRule,
			}
		}
		appConfs[appName] = appRules

	}

	return &OutputConfig{
		OutputConfig: config.OutputConfig{
			CommonConfig: config.CommonConfig{
				Type: ModuleName,
			},
		},
		Address: address,

		AppConfigs: appConfs,
	}, nil
}

// InitHandler initialize the output plugin
func InitHandler(ctx context.Context, raw *config.ConfigRaw) (config.TypeOutputConfig, error) {
	conf, err := initConfig()
	if err != nil {
		return nil, err
	}

	// register metric for each app
	for _, appConf := range conf.AppConfigs {
		for _, rule := range appConf {
			if err := prometheus.Register(rule.MsgMetric); err != nil {
				return nil, err
			}
		}
	}

	go conf.serveHTTP()

	return conf, nil
}

// Output event
func (o *OutputConfig) Output(ctx context.Context, event logevent.LogEvent) (err error) {
	msg := event.Message
	// filter by app name
	appName := event.GetString(appNameField)

	if rules, ok := o.AppConfigs[appName]; ok {
		// check each rule in the app config
		for _, v := range rules {
			// find regex from memory
			r, ok := regexMap[v.Name]
			if !ok {
				// compile new regex if not found
				r = regexp.MustCompile(v.Regex)
			}

			msgMetric := v.MsgMetric
			metricType := v.MetricType

			if r.MatchString(msg) {
				switch metricType {
				case counter:
					// for counter type metric
					msgMetric.(prometheus.Counter).Inc()

				case gauge:
					// for gauge type metric
					// filter gauge number from message
					numStr := r.ReplaceAllLiteralString(msg, "")
					s, err := strconv.ParseFloat(numStr, 64)
					if err != nil {
						return err
					}

					msgMetric.(prometheus.Gauge).Set(s)

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
