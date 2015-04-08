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

from prodiguer import db
from prodiguer.api.utils import handler as hu



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
            'simulation_state_history': db.utils.get_list(db.types.SimulationState)
            }

        # End db session.
        db.session.end()

        # Write response.
        hu.write_json_response(self, data)
