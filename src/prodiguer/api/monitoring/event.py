# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.monitoring.event.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring event request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

import tornado.web

from prodiguer import db
from prodiguer.api.utils import ws
from prodiguer.utils import config, rt, string_convertor



# Key used for web socket cache and logging.
_WS_KEY = 'monitoring'


def _log(msg):
    """Logging utilty function.

    """
    rt.log_api("{0} :: {1}".format(_WS_KEY, msg))


def _get_simulation_state_change_data(handler):
    """Event publisher: simulation state change.

    """
    return {
        'uid': handler.get_argument('uid'),
        'state' : handler.get_argument('state')
        }


def _get_simulation_termination_data(handler):
    """Event publisher: simulation termination.

    """
    return {
        'ended': handler.get_argument('ended'),
        'uid': handler.get_argument('uid'),
        'state' : handler.get_argument('state')
        }


def _get_new_simulation_data(handler):
    """Event publisher: new simulation.

    """
    # Load simulation.
    simulation_uid = handler.get_argument('uid')
    simulation = db.dao_monitoring.retrieve_simulation(simulation_uid)
    if simulation is None:
        raise ValueError("Unknown simulation: {}".format(simulation_uid))

    # Initialise event data.
    return {
        'simulation': simulation
        }


# Map of event data factory functions.
_DATA_FACTORIES = {
    'new_simulation': _get_new_simulation_data,
    'simulation_state_change': _get_simulation_state_change_data,
    'simulation_termination': _get_simulation_termination_data
}


class EventRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring event request handler.

    """
    @tornado.web.asynchronous
    def get(self, *args):
        """HTTP GET handler.

        """
        # Signal asynch.
        self.finish()

        # Set event type / data factory.
        event = self.get_argument('event_type')
        _log("{0} event received: {1}".format(event, self.request.arguments))

        # Connect to db.
        db.session.start(config.db.pgres.main)

        # Set data to be published.
        data_factory = _DATA_FACTORIES[event]
        data = data_factory(self)
        data.update({
            'event_type' : string_convertor.to_camel_case(event),
            'event_timestamp': datetime.datetime.now(),
        })

        # End db session.
        db.session.end()

        # Broadcast event to clients.
        ws.on_write(_WS_KEY, data)
        _log("{0} event published".format(event))
