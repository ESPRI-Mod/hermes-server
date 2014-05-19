# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.monitoring.event.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring event request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import datetime

import tornado.web

from . import utils
from .. import utils_ws as ws
from .... import db
from .... utils import (
    convert, 
    runtime as rt
    )



# Key used for web socket cache and logging.
_KEY = 'monitoring'


def _log(msg):
    """Logging utilty function."""
    rt.log_api("{0} :: {1}".format(_KEY, msg))


def _broadcast_on_state_change_event(handler):
    """Web socket event broadcast :: simulation state change."""
    ws.on_write(_KEY, { 
        'eventType' : 'stateChange',
        'eventTimestamp': unicode(datetime.datetime.now()),
        'id': int(handler.get_argument('id')), 
        'state' : handler.get_argument('state')
        })


def _broadcast_on_new_event(handler):
    """Web socket event broadcast :: new simulation."""
    # Load simulation.
    s = db.dao.get_by_id(db.types.Simulation, int(handler.get_argument('id')))

    # Format simulation.
    s = utils.get_simulation_dict(s)
    s = convert.dict_keys(s, convert.str_to_camel_case)

    # Write web-socket event.
    ws.on_write(_KEY, {
        'eventType' : 'new',
        'eventTimestamp': unicode(datetime.datetime.now()),
        'simulation': s
        })


# Map of supported broadcasters.
_broadcasters = {
    'state_change' : _broadcast_on_state_change_event,
    'new' : _broadcast_on_new_event
}


class EventRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring event request handler.

    """
    @tornado.web.asynchronous
    def get(self, *args):
        self.finish()

        _log("on {0} event received".format(self.get_argument('event_type')))

        # Broadcase event to clients.
        _broadcasters[self.get_argument('event_type')](self)

        _log("on {0} event broadcast".format(self.get_argument('event_type')))


    @tornado.web.asynchronous
    def post(self):
        pass
