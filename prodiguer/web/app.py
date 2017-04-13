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
from prodiguer.web.endpoints import metrics_pcmdi
from prodiguer.web.utils import websockets



# Base address to API endpoints.
_BASE_ADDRESS = '{}/api/1{}'


def _get_path_to_front_end():
    """Return path to the front end javascript application.

    """
    dir_fe = shell.get_repo_path(['hermes-fe', 'src'])
    log("Front-end static files @ {}".format(dir_fe))

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
        (r'/api/1/simulation/metrics/add', metrics_pcmdi.AddRequestHandler),
        (r'/api/1/simulation/metrics/delete', metrics_pcmdi.DeleteRequestHandler),
        (r'/api/1/simulation/metrics/fetch', metrics_pcmdi.FetchRequestHandler),
        (r'/api/1/simulation/metrics/fetch_count', metrics_pcmdi.FetchCountRequestHandler),
        (r'/api/1/simulation/metrics/fetch_columns', metrics_pcmdi.FetchColumnsRequestHandler),
        (r'/api/1/simulation/metrics/fetch_list', metrics_pcmdi.FetchListRequestHandler),
        (r'/api/1/simulation/metrics/fetch_setup', metrics_pcmdi.FetchSetupRequestHandler),
        (r'/api/1/simulation/metrics/rename', metrics_pcmdi.RenameRequestHandler),
        (r'/api/1/simulation/metrics/set_hashes', metrics_pcmdi.SetHashesRequestHandler)
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
        log("{} ---> {}".format(url, str(handler).split(".")[-1][0:-2]))

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
    app.listen(int(config.web.port))

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


def get_endpoint(suffix):
    """Returns the endpoint prefixed with base address.
    :param str suffix: Endpoint suffix.
    :returns: The endpoint prefixed with base address.
    :rtype: str
    """
    return _BASE_ADDRESS.format(config.web.url, suffix)
