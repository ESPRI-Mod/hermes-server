# -*- coding: utf-8 -*-

# Module imports.
import os

import tornado
from tornado.web import Application


from . handlers import (
    metric,
    monitoring,
    ops,
    )

from .. utils import (
    config as cfg,
    runtime as rt
    )


def _get_path_to_front_end():
    """Return path to the front end javascript application."""
    # N.B. deriving path on assumption that software
    # has been installed using the prodiguer bootstrapper.

    # Get directory to prodiguer repos.
    repos = os.path.dirname(__file__)
    for i in range(4):
        repos = os.path.dirname(repos)

    # Get directory to front end source code.
    path = os.path.join(repos, 'prodiguer-fe')
    path = os.path.join(path, 'src')
    rt.log_api("Front-end @ {0}".format(path))

    return path


def _get_app_routes():
    """Returns supported app routes."""
    return (
        # Monitoring routes.
        (r'/api/1/monitoring/fe/setup', monitoring.FrontEndSetupRequestHandler),
        (r'/api/1/monitoring/fe/ws', monitoring.FrontEndWebSocketHandler),
        (r'/api/1/monitoring/event', monitoring.EventRequestHandler),
        # Metric routes.
        (r'/api/1/metric/add', metric.AddRequestHandler),
        (r'/api/1/metric/delete', metric.DeleteRequestHandler),
        (r'/api/1/metric/delete_lines', metric.DeleteLinesRequestHandler),
        (r'/api/1/metric/fetch', metric.FetchRequestHandler),
        (r'/api/1/metric/fetch_count', metric.FetchCountRequestHandler),
        (r'/api/1/metric/fetch_columns', metric.FetchColumnsRequestHandler),
        (r'/api/1/metric/fetch_list', metric.FetchListRequestHandler),
        (r'/api/1/metric/fetch_setup', metric.FetchSetupRequestHandler),
        # Operational routes.
        (r'/api/1/ops/heartbeat', ops.HeartbeatRequestHandler),
        (r'/api/1/ops/list_endpoints', ops.ListEndpointsRequestHandler),
    )


def _get_app_settings():
    """Returns app settings."""
    return {
        "cookie_secret": cfg.api.cookie_secret,
        "static_path": _get_path_to_front_end()
    }


def run():
    """Runs the prodiguer web api."""
    # Setup app.
    rt.log_api("Initializing")

    # Instantiate.
    app = Application(_get_app_routes(),
                      debug=not cfg.api.mode=='prod',
                      **_get_app_settings())

    # Listen.
    app.listen(cfg.api.port)
    rt.log_api("Ready")

    # Start io loop.
    tornado.ioloop.IOLoop.instance().start()



# Main entry point.
if __name__ == '__main__':
    run()

