import logging
from appmetrics import metrics
from appmetrics.histogram import SlidingTimeWindowReservoir
from flask import Flask

logger = logging.getLogger('unicon.metrics')


def generate_metric_name(app_module: str = "app", metric_name: str = "undefined"):
    from environment import app_version, app_location, settings
    metric_name = f"{settings['organization']}_{app_location}_{app_version}_{app_module}_{metric_name}"
    if "." in metric_name:
        metric_name = metric_name.replace(".", "-")
    return metric_name


def metric_notify_counter(app_module, metric_name, count=None, is_init=False):
    metric_name = generate_metric_name(app_module, metric_name)
    if not metrics.REGISTRY.get(metric_name):
        metrics.new_counter(metric_name)
    if is_init:
        return
    if not count:
        metrics.notify(metric_name, 1)
    else:
        metrics.notify(metric_name, count)


def metric_notify_gauge(app_module, metric_name, value):
    metric_name = generate_metric_name(app_module, metric_name)
    if not metrics.REGISTRY.get(metric_name):
        metrics.new_gauge(metric_name)
    metrics.notify(metric_name, value)


def run_metrics_webserver(host: str = '0.0.0.0', port: int = 5000):
    log = logging.getLogger('werkzeug')
    log.setLevel(logging.ERROR)

    app = Flask(__name__)
    from appmetrics.wsgi import AppMetricsMiddleware
    app.wsgi_app = AppMetricsMiddleware(app.wsgi_app, "app_metrics")
    app.run(host, port, debug=False)
