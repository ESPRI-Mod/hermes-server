# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.monitoring.event.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring event request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime
import json

import tornado.web

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer.db.pgres import dao_monitoring_ll as dao_ll
from prodiguer.utils import logger
from prodiguer.utils import string_convertor as sc
from prodiguer.web.utils import websockets




# Key used for web socket cache and logging.
_WS_KEY = 'monitoring'


def _log(msg):
    """Helper: logging.

    """
    logger.log_web("{0} :: {1}".format(_WS_KEY, msg))


def _get_simulation_event_data(request_data):
    """Event data factory: returns simulation event data.

    """
    simulation_uid = request_data['simulation_uid']
    simulation = dao.retrieve_simulation(simulation_uid)
    if simulation:
        return {
            'cv_terms': request_data.get('cv_terms', []),
            'job_list': dao_ll.retrieve_simulation_jobs(simulation_uid),
            'simulation': simulation,
            'simulation_uid': simulation_uid
            }


def _get_job_event_data(request_data):
    """Event data factory: returns job event data.

    """
    job_uid = request_data['job_uid']
    job = dao_ll.retrieve_job(job_uid)
    if job:
        return {
            'job': job,
            'simulation_uid': request_data['simulation_uid']
        }


class _EventManager(object):
    """Encpasulates incoming event information.

    """
    def __init__(self, handler):
        """Object initializer.

        """
        self.request_data = json.loads(handler.request.body)
        self.type = self.request_data['event_type']
        if self.request_data['event_type'].startswith("simulation"):
            self.data_factory = _get_simulation_event_data
        else:
            self.data_factory = _get_job_event_data
        _log("{0} event received: {1}".format(self.type, self.request_data))


    def get_websocket_data(self):
        """Returns data to be dispatched to web-socket clients.

        """
        return self.data_factory(self.request_data)


def _ws_client_filter(client, data):
    """Determines whether the data will be pushed to the web-socket client.

    """
    simulation_uid = client.get_argument("simulationUID", None)
    if simulation_uid is None:
        return True
    else:
        print "DSDS", data
        return data['simulation_uid'] == simulation_uid


class EventRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring web socket event request handler.

    """
    @tornado.web.asynchronous
    def post(self):
        """HTTP POST handler.

        """
        # Signal asynch.
        self.finish()

        # Connect to db.
        db.session.start()

        try:
            event = _EventManager(self)
            data = event.get_websocket_data()
            if data is not None:
                data.update({
                    'event_type' : sc.to_camel_case(event.type),
                    'event_timestamp': datetime.datetime.now(),
                })
                websockets.on_write(_WS_KEY, data, _ws_client_filter)

        # Close db session.
        finally:
            db.session.end()
