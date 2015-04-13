# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.monitoring.event.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring event request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime, json

import tornado.web

from prodiguer.api import utils_ws
from prodiguer.db import pgres as db
from prodiguer.utils import (
    config,
    rt,
    string_convertor as sc,
    data_convertor as dc
    )



# Key used for web socket cache and logging.
_WS_KEY = 'monitoring'


def _log(msg):
    """Helper function: logging.

    """
    rt.log_api("{0} :: {1}".format(_WS_KEY, msg))


def _on_simulation_state_change(data):
    """Event handler: simulation state change.

    """
    # Load simulation state history.
    state_history = db.dao_monitoring.retrieve_simulation_states(data['uid'])
    if not state_history:
        raise ValueError("Unknown simulation states: {}".format(data['uid']))

    return {
        'simulation_state_history' : state_history,
        'simulation_uid': data['uid']
        }


def _on_new_simulation(data):
    """Event handler: new simulation.

    """
    # Load simulation.
    simulation = db.dao_monitoring.retrieve_simulation(data['uid'])
    if not simulation:
        raise ValueError("Unknown simulation: {}".format(data['uid']))

    # Load simulation state history.
    state_history = db.dao_monitoring.retrieve_simulation_states(data['uid'])
    if not state_history:
        raise ValueError("Unknown simulation states: {}".format(data['uid']))

    # Initialise event data.
    return {
        'cv_terms': data['cv_terms'],
        'simulation': simulation,
        'simulation_state_history': state_history
        }


# Map of event handlers.
_EVENT_HANDLERS = {
    'new_simulation': _on_new_simulation,
    'simulation_state_change': _on_simulation_state_change
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
        handler = _EVENT_HANDLERS[self.type]

        return handler(self.data)


class EventRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring event request handler.

    """
    @tornado.web.asynchronous
    def post(self):
        """HTTP POST handler.

        """
        # Signal asynch.
        self.finish()

        # Escape if there are no connected clients.
        # if not utils_ws.get_client_count(_WS_KEY):
        #     return

        # Connect to db.
        db.session.start()

        try:
            # Decode event information.
            event = _EventInfo(self.request.body)
            _log("{0} event received: {1}".format(event.type, event.data))

            # Set data to be dispatched to web-socket clients.
            ws_data = event.get_ws_data()
            ws_data.update({
                'event_type' : sc.to_camel_case(event.type),
                'event_timestamp': datetime.datetime.now(),
            })

            # Broadcast event data to clients.
            utils_ws.on_write(_WS_KEY, ws_data)
            _log("{0} event published".format(event.type))

        # Close db session.
        finally:
            db.session.end()
