# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.monitoring.setup.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.web

from prodiguer.api import utils_handler
from prodiguer.db import pgres as db



# Set of states to exclude from pushing to front end.
_STATE_BLACKLIST = {u'q-in-monitoring-9000'}


def _get_simulation_state_history():
    """Returns set of simulation state history entries for passing to UI.

    """
    states = db.utils.get_list(db.types.SimulationState)

    return [s for s in states if s[u'info'] not in _STATE_BLACKLIST]


class FrontEndSetupRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        # Start db session.
        db.session.start()

        # Load setup data from db.
        data = {
            'cv_terms': db.utils.get_list(db.types.ControlledVocabularyTerm),
            'simulation_list': db.utils.get_list(db.types.Simulation),
            'simulation_state_history': _get_simulation_state_history()
            }

        # End db session.
        db.session.end()

        # Write response.
        utils_handler.write_json_response(self, data)
