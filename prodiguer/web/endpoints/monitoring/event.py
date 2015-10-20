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
from prodiguer.utils import logger
from prodiguer.utils import string_convertor as sc
from prodiguer.web.request_validation import validator_monitoring as rv
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
    # Unpack event data.
    simulation_uid = request_data['simulation_uid']

    # Load simulation (if not found then do not broadcast).
    simulation = db.dao_monitoring.retrieve_simulation(simulation_uid)
    if simulation is None:
        return None

    # Load simulation jobs.
    jobs = db.dao_monitoring.retrieve_simulation_jobs(simulation_uid)

    print "NEW CV TERMS ::", request_data.get('cv_terms', [])
    print "NEW CV TERMS ::", request_data.get('cvTerms', [])

    # Return event data.
    return {
        'cv_terms': request_data.get('cv_terms', []),
        'job_list': jobs,
        'simulation': simulation,
        'simulation_uid': simulation.uid
        }


def _get_job_event_data(request_data):
    """Event data factory: returns job event data.

    """
    # Unpack event data.
    job_uid = request_data['job_uid']

    # Load job (if not found then do not broadcast).
    job = db.dao_monitoring.retrieve_job(job_uid)
    if job is None:
        return None

    # Return event data.
    return {
        'job': job,
        'simulation_uid': job.simulation_uid
        }


# Map of event type to data factory.
_EVENT_DATA_FACTORIES = {
    'simulation_start': _get_simulation_event_data,
    'simulation_complete': _get_simulation_event_data,
    'simulation_error': _get_simulation_event_data,
    'job_start': _get_job_event_data,
    'job_complete': _get_job_event_data,
    'job_error': _get_job_event_data
}


class _EventManager(object):
    """Encpasulates incoming event information.

    """
    def __init__(self, handler):
        """Object initializer.

        """
        self.request_data = json.loads(handler.request.body)
        self.type = self.request_data['event_type']
        self.data_factory = _EVENT_DATA_FACTORIES[self.type]


    def get_websocket_data(self):
        """Returns data to be dispatched to web-socket clients.

        """
        return self.data_factory(self.request_data)


class EventRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring web socket event request handler.

    """
    @tornado.web.asynchronous
    def post(self):
        """HTTP POST handler.

        """
        def _ws_client_filter(client, data):
            """Determines whether the data will be pushed to the web-socket client.

            """
            simulation_uid = client.get_argument("simulationUID", None)
            if simulation_uid is None:
                return True
            else:
                return data['simulation_uid'] == simulation_uid


        # Signal asynch.
        self.finish()

        # Connect to db.
        db.session.start()

        try:
            # Instantiate event manager.
            event = _EventManager(self)
            _log("{0} event received: {1}".format(event.type, event.request_data))

            # Only broadcast when there is data.
            data = event.get_websocket_data()
            if not data:
                _log("{0} event broadcasting aborted".format(event.type))
            else:
                # Append event info.
                data.update({
                    'event_type' : sc.to_camel_case(event.type),
                    'event_timestamp': datetime.datetime.now(),
                })

                # Broadcast to clients.
                websockets.on_write(_WS_KEY, data, _ws_client_filter)

        # Close db session.
        finally:
            db.session.end()
