# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.monitoring.setup.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import tornado.web

from . import utils
from .... import db
from ....utils import convert, config



def _get_simulation_list():
    """Returns list of simulations from db.

    """
    collection = db.dao.get_all(db.types.Simulation)

    return [utils.get_simulation_dict(s) for s in collection]


def _get_simulation_state_change_list():
    """Returns list of simulation state change events from db.

    """
    collection = db.dao.get_all(db.types.SimulationStateChange)

    return [utils.get_simulation_state_change_dict(ssc) for ssc in collection]


class FrontEndSetupRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring front end setup request handler.

    """
    def get(self, *args):
        # Start session.
        db.session.start(config.db.pgres.main)

        # Load setup data from db.
        data = {
            'simulation_list': _get_simulation_list(),
            'simulation_state_change_list': _get_simulation_state_change_list()
        }
        data.update(utils.get_simulation_filter_facets())

        # Convert keys to camel case so as to respect json naming conventions.
        data = convert.dict_keys(data, convert.str_to_camel_case)

        self.write(data)

