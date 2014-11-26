# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.monitoring.event.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring event request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import datetime

import tornado.web

from . import utils
from .. import utils_ws as ws
from .... import db
from .... utils import config, convert, rt



# Key used for web socket cache and logging.
_KEY = 'monitoring'


def _log(msg):
    """Logging utilty function.

    """
    rt.log_api("{0} :: {1}".format(_KEY, msg))


def _publish(data):
    """Publishes event data over web socket channel.

    """
    ws.on_write(_KEY, data)


def _publish_simulation_state_change(handler):
    """Event publisher: simulation state change.

    """
    data = {
        'eventType' : 'stateChange',
        'eventTimestamp': unicode(datetime.datetime.now()),
        'uid': unicode(handler.get_argument('uid')),
        'state' : handler.get_argument('state')
        }

    _publish(data)


def _publish_simulation_termination(handler):
    """Event publisher: simulation termination.

    """
    data = {
        'ended': unicode(handler.get_argument('ended')[:10]),
        'eventType' : 'simulationTermination',
        'eventTimestamp': unicode(datetime.datetime.now()),
        'uid': unicode(handler.get_argument('uid')),
        'state' : handler.get_argument('state')
        }

    _publish(data)


def _publish_new_simulation(handler):
    """Event publisher: new simulation.

    """
    # Load & format simulation.
    simulation_id = int(handler.get_argument('id'))
    simulation = db.dao.get_by_id(db.types.Simulation, simulation_id)
    simulation = utils.get_simulation_dict(simulation)
    simulation = convert.dict_keys(simulation, convert.str_to_camel_case)

    # Set event data.
    data = {
        'eventType' : 'new',
        'eventTimestamp': unicode(datetime.datetime.now()),
        'simulation': simulation
        }
    if 'new_cv_terms' in handler.request.arguments:
        db.cache.reload()
        # terms = [[type(t).__name__, db.types.Convertor.to_dict(t)] for t in terms]
        data['newCVTerms'] = handler.get_argument('new_cv_terms')

    # Publish event.
    _publish(data)


# Map of supported event publishers.
_publishers = {
    'new_simulation': _publish_new_simulation,
    'simulation_state_change': _publish_simulation_state_change,
    'simulation_termination': _publish_simulation_termination
}


class EventRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring event request handler.

    """
    @tornado.web.asynchronous
    def get(self, *args):
        # Signal asynch.
        self.finish()

        # Set event type.
        event_type = self.get_argument('event_type')
        _log("on {0} event received: {1}".format(event_type, self.request.arguments))

        # Set publisher.
        publisher = _publishers[event_type]

        # Start session.
        db.session.start(config.db.pgres.main)

        # Publish event to clients.
        publisher(self)
        _log("on {0} event published".format(event_type))
