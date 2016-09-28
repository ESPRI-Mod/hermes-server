# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.app.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Web service application entry point.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.web

from prodiguer.cv import session as cv_session
from prodiguer.utils import config
from prodiguer.utils import shell
from prodiguer.utils.logger import log_web as log
from prodiguer.web import schemas
from prodiguer.web.endpoints import cv
from prodiguer.web.endpoints import monitoring
from prodiguer.web.endpoints import ops
from prodiguer.web.endpoints import sim_metrics
from prodiguer.web.utils import websockets


def _get_path_to_front_end():
    """Return path to the front end javascript application.

    """
    dir_fe = shell.get_repo_path(['hermes-fe', 'src'])
    log("Front-end static files @ {0}".format(dir_fe))

    return dir_fe


def _get_app_endpoints():
    """Returns map of application endpoints to handlers.

    """
    return (
        # Operations routes.
        (r'/api', ops.HeartbeatRequestHandler),
        # Controlled vocabulary routes.
        (r'/api/1/cv/fetch', cv.FetchRequestHandler),
        # Simulation monitoring routes.
        (r'/api/1/simulation/monitoring/fetch_detail', monitoring.FetchDetailRequestHandler),
        (r'/api/1/simulation/monitoring/fetch_messages', monitoring.FetchMessagesRequestHandler),
        (r'/api/1/simulation/monitoring/fetch_timeslice', monitoring.FetchTimeSliceRequestHandler),
        (r'/api/1/simulation/monitoring/ws/all', monitoring.FrontEndWebSocketAllHandler),
        (r'/api/1/simulation/monitoring/event', monitoring.EventRequestHandler),
        # Simulation metric routes.
        (r'/api/1/simulation/metrics/add', sim_metrics.AddRequestHandler),
        (r'/api/1/simulation/metrics/delete', sim_metrics.DeleteRequestHandler),
        (r'/api/1/simulation/metrics/fetch', sim_metrics.FetchRequestHandler),
        (r'/api/1/simulation/metrics/fetch_count', sim_metrics.FetchCountRequestHandler),
        (r'/api/1/simulation/metrics/fetch_columns', sim_metrics.FetchColumnsRequestHandler),
        (r'/api/1/simulation/metrics/fetch_list', sim_metrics.FetchListRequestHandler),
        (r'/api/1/simulation/metrics/fetch_setup', sim_metrics.FetchSetupRequestHandler),
        (r'/api/1/simulation/metrics/rename', sim_metrics.RenameRequestHandler),
        (r'/api/1/simulation/metrics/set_hashes', sim_metrics.SetHashesRequestHandler)
    )


def _get_app_settings():
    """Returns app settings.

    """
    return {
        "cookie_secret": config.web.cookie_secret,
        "compress_response": True,
        "static_path": _get_path_to_front_end()
    }


def _is_in_debug_mode():
    """Returns app debug mode.

    """
    return config.deploymentMode == 'dev'


def _get_app():
    """Returns application instance.

    """
    endpoints = _get_app_endpoints()
    log("Endpoint to handler mappings:")
    for url, handler in sorted(endpoints, key=lambda i: i[0]):
        log("{0} ---> {1}".format(url, str(handler).split(".")[-1][0:-2]))

    schemas.init([i[0] for i in endpoints])

    return tornado.web.Application(endpoints,
                                   debug=_is_in_debug_mode(),
                                   **_get_app_settings())


def run():
    """Runs the prodiguer web api.

    """
    # Initialize application.
    log("Initializing")

    # Initialise cv session.
    cv_session.init()

    # Instantiate.
    app = _get_app()

    # Open port.
    app.listen(config.web.port)

    # Set web-socket keep alive.
    websockets.keep_alive()
    log("Ready")

    # Start processing requests.
    tornado.ioloop.IOLoop.instance().start()


def stop():
    """Stops web service.

    """
    ioloop = tornado.ioloop.IOLoop.instance()
    ioloop.add_callback(lambda x: x.stop(), ioloop)
