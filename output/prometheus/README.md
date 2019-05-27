# gogstash prometheus plugin

* gogstash first loads each app's configuration from **protoconf**
* it will check the input from **filebeat**
* **prometheus** collectors collect the stats if matches the regexes for each app
* output to prometheus
* probably add rules in prometheus and trigger **alert manager** when exceeding the threshold

## configure in 深水

* you can configure the prometheus metrics output in **深水**
* `app_name` is log_topics field in filebeat configuration which to identify the app, like ``deepWater``
* add new prometheus metrics collector
  * `metric_type`: ``counter=0``, ``gauge=1``
  * `metric_name` is the identifier of the metric, in prometheus full name will be [app_name]_[metric_name], like ``deepWater_status_400``
  * `regex` is the pattern to match in filebeat output: like `"status":400` is **"status":400** or `[0-9]{4}.(0[1-9]|1[0-2]).(0[1-9]|[1-2][0-9]|3[0-1])\s(2[0-3]|[01][0-9]):[0-5][0-9]:[0-5][0-9]\sOnline\sUser:\s` is **2019.05.06 11:23:41 Online User: 1234**
