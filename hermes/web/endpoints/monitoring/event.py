# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.monitoring.event.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring event request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime
import json

import tornado.web

from hermes.db import pgres as db
from hermes.db.pgres.dao_monitoring import retrieve_job_info
from hermes.db.pgres.dao_monitoring import retrieve_latest_job_period
from hermes.db.pgres.dao_monitoring import retrieve_simulation
from hermes.db.pgres.dao_monitoring import retrieve_simulation_job_counts
from hermes.db.pgres.dao_monitoring import retrieve_simulation_latest_job
from hermes.utils import logger
from hermes.utils import string_convertor as sc
from hermes.web.utils import websockets




# Key used for web socket cache and logging.
_WS_KEY = 'monitoring'


def _log(msg):
    """Helper: logging.

    """
    logger.log_web("{0} :: {1}".format(_WS_KEY, msg))


def _get_simulation_event_data(request_data):
    """Event data factory: returns simulation event data.

    """
    uid = request_data['simulation_uid']
    with db.session.create():
        simulation = retrieve_simulation(uid)
        if simulation is not None:
            return {
                'cv_terms': request_data.get('cv_terms', []),
                'latest_compute_job': retrieve_simulation_latest_job(uid),
                'job_counts': retrieve_simulation_job_counts(uid),
                'job_period': retrieve_latest_job_period(uid),
                'simulation': simulation,
                'simulation_uid': uid
                }


def _get_job_event_data(request_data):
    """Event data factory: returns job event data.

    """
    with db.session.create():
        job = retrieve_job_info(request_data['job_uid'])
    if job:
        return {
            'job': job,
            'simulation_uid': request_data['simulation_uid']
        }


def _get_job_period_event_data(request_data):
    """Event data factory: returns job period event data.

    """
    return {
        'simulation_uid': request_data['simulation_uid'],
        'start_date': request_data['period_date_begin']
    }


class _EventManager(object):
    """Encpasulates incoming event information.

    """
    def __init__(self, handler):
        """Object initializer.

        """
        self.request_data = json.loads(handler.request.body)
        self.type = self.request_data['event_type']
        if self.type.startswith("simulation"):
            self.data_factory = _get_simulation_event_data
        elif self.type.startswith("job_period"):
            self.data_factory = _get_job_period_event_data
        elif self.type.startswith("job"):
            self.data_factory = _get_job_event_data


    def get_websocket_data(self):
        """Returns data to be dispatched to web-socket clients.

        """
        return self.data_factory(self.request_data)


def _ws_client_filter(client, data):
    """Determines whether the data will be pushed to the web-socket client.

    """
    simulation_uid = client.get_argument("simulationUID", None)

    return simulation_uid is None or \
           simulation_uid == data['simulation_uid']


class EventRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring web socket event request handler.

    """
    @tornado.web.asynchronous
    def post(self):
        """HTTP POST handler.

        """
        # Signal asynch.
        self.finish()

        # Escape if no active web-socket connections.
        # if websockets.get_client_count() == 0:
        #     return

        # Set event manager.
        event = _EventManager(self)
        _log("{0} event received: {1}".format(event.type, event.request_data))

        # Set event data.
        data = event.get_websocket_data()

        # Publish web-socket event.
        if data is not None:
            data.update({
                'event_type' : sc.to_camel_case(event.type),
                'event_timestamp': datetime.datetime.utcnow(),
            })
            websockets.on_write(_WS_KEY, data, _ws_client_filter)
