# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.monitoring.event.py
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
from prodiguer.web import utils_ws



# Key used for web socket cache and logging.
_WS_KEY = 'monitoring'


def _log(msg):
    """Helper: logging.

    """
    logger.log_web("{0} :: {1}".format(_WS_KEY, msg))


def _get_simulation_event_data(data):
    """Helper: retuns common simulation event data.

    """
    # Unpack event data.
    simulation_uid = data['simulation_uid']

    # Load simulation (if not found then do not broadcast).
    simulation = db.dao_monitoring.retrieve_simulation(simulation_uid)
    if not simulation:
        return None

    # Load simulation jobs.
    jobs = db.dao_monitoring.retrieve_simulation_jobs(simulation_uid)

    # Return event data.
    return {
        'cv_terms': data.get('cv_terms', []),
        'simulation': simulation,
        'job_history': jobs
        }


# Map of event type to event handlers.
_EVENT_HANDLERS = {
    'simulation_start': _get_simulation_event_data,
    'simulation_complete': _get_simulation_event_data,
    'simulation_error': _get_simulation_event_data,
    'job_start': _get_simulation_event_data,
    'job_complete': _get_simulation_event_data,
    'job_error': _get_simulation_event_data
}


class _EventInfo(object):
    """Encpasulates incoming event information.

    """
    def __init__(self, request_body):
        """Object initializer.

        """
        self.data = json.loads(request_body)
        self.type = self.data['event_type']
        self.handler = _EVENT_HANDLERS[self.type]


    def get_ws_data(self):
        """Returns data to be dispatched to web-socket clients.

        """
        return self.handler(self.data)


class EventRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring event request handler.

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
            # Decode event information.
            event = _EventInfo(self.request.body)
            _log("{0} event received: {1}".format(event.type, event.data))

            # Only broadcast when there is data.
            ws_data = event.get_ws_data()
            if not ws_data:
                _log("{0} event broadcasting aborted".format(event.type))
            else:
                # Append event info.
                ws_data.update({
                    'event_type' : sc.to_camel_case(event.type),
                    'event_timestamp': datetime.datetime.now(),
                })

                # Broadcast to clients.
                utils_ws.on_write(_WS_KEY, ws_data)
                _log("{0} event broadcast".format(event.type))

        # Close db session.
        finally:
            db.session.end()
