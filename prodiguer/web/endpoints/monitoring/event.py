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

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring as dao
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
        data = {
            'cv_terms': request_data.get('cv_terms', []),
            'latest_compute_job': dao.retrieve_simulation_latest_job(simulation_uid),
            'job_counts': dao.retrieve_simulation_job_counts(simulation_uid),
            'job_period': dao.retrieve_latest_job_period(simulation_uid),
            'simulation': simulation,
            'simulation_uid': simulation_uid
            }

        return data


def _get_job_event_data(request_data):
    """Event data factory: returns job event data.

    """
    job_uid = request_data['job_uid']
    job = dao.retrieve_job_subset(job_uid)
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

        if self.request_data['event_type'].startswith("simulation"):
            self.data_factory = _get_simulation_event_data
        elif self.request_data['event_type'].startswith("job_period"):
            self.data_factory = _get_job_period_event_data
        elif self.request_data['event_type'].startswith("job"):
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

        # Write web-socket event data.
        with db.session.create():
            event = _EventManager(self)
            data = event.get_websocket_data()
            if data is not None:
                data.update({
                    'event_type' : sc.to_camel_case(event.type),
                    'event_timestamp': datetime.datetime.utcnow(),
                })
                websockets.on_write(_WS_KEY, data, _ws_client_filter)
