# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.app.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Web service application entry point.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado
from tornado.web import Application


from prodiguer import cv
from prodiguer.web import aggregations
from prodiguer.web import monitoring
from prodiguer.web import ops
from prodiguer.web import sim_metrics
from prodiguer.web import utils_ws
from prodiguer.utils import config
from prodiguer.utils import logger
from prodiguer.utils import rt



def _get_path_to_front_end():
    """Return path to the front end javascript application.

    """
    dir_fe = rt.get_path_to_repo(['prodiguer-fe', 'src'])
    logger.log_web("Front-end static files @ {0}".format(dir_fe))

    return dir_fe


def _get_app_routes():
    """Returns supported app routes.

    """
    return (
        # Aggregation routes.
        # (r'/api/1/aggregations/find', aggregations.FindRequestHandler),
        # Monitoring routes.
        (r'/api/1/monitoring/fe/cv', monitoring.FrontEndControlledVocabularyRequestHandler),
        (r'/api/1/monitoring/fe/setup/all', monitoring.FrontEndSetupAllRequestHandler),
        (r'/api/1/monitoring/fe/setup/one', monitoring.FrontEndSetupOneRequestHandler),
        (r'/api/1/monitoring/fe/ws/all', monitoring.FrontEndWebSocketAllHandler),
        (r'/api/1/monitoring/event', monitoring.EventRequestHandler),
        # Metric routes.
        (r'/api/1/metric/add', sim_metrics.AddRequestHandler),
        (r'/api/1/metric/delete', sim_metrics.DeleteRequestHandler),
        (r'/api/1/metric/fetch', sim_metrics.FetchRequestHandler),
        (r'/api/1/metric/fetch_count', sim_metrics.FetchCountRequestHandler),
        (r'/api/1/metric/fetch_columns', sim_metrics.FetchColumnsRequestHandler),
        (r'/api/1/metric/fetch_list', sim_metrics.FetchListRequestHandler),
        (r'/api/1/metric/fetch_setup', sim_metrics.FetchSetupRequestHandler),
        (r'/api/1/metric/rename', sim_metrics.RenameRequestHandler),
        (r'/api/1/metric/set_hashes', sim_metrics.SetHashesRequestHandler),
        # Operational routes.
        (r'/api/1/ops/heartbeat', ops.HeartbeatRequestHandler)
    )


def _get_app_settings():
    """Returns app settings.

    """
    return {
        "cookie_secret": config.web.cookie_secret,
        "compress_response": True,
        "static_path": _get_path_to_front_end()
    }


def _get_app_debug_mode():
    """Returns app debug mode.

    """
    return config.deploymentMode=='dev'


def run():
    """Runs the prodiguer web api.

    """
    # Setup app.
    logger.log_web("Initializing")

    # Initialise cv session.
    cv.session.init()

    # Instantiate.
    app = Application(_get_app_routes(),
                      debug=_get_app_debug_mode(),
                      **_get_app_settings())

    # Listen.
    app.listen(int(config.web.host.split(":")[1]))

    # Set web-socket keep alive.
    utils_ws.keep_alive()

    # Start io loop.
    logger.log_web("Ready")
    tornado.ioloop.IOLoop.instance().start()



# Main entry point.
if __name__ == '__main__':
    run()

