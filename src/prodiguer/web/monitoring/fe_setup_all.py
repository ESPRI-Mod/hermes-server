# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.monitoring.setup.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.web

from prodiguer.web import utils_handler
from prodiguer.db import pgres as db



def _get_data(func):
    """Returns data for front-end.

    """
    return db.utils.get_collection(func())


class FrontEndSetupAllRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring all simulations front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        # Start db session.
        db.session.start()

        # Load setup data from db.
        data = {
            'job_history':
                _get_data(db.dao_monitoring.retrieve_active_jobs),
            'simulation_list':
                _get_data(db.dao_monitoring.retrieve_active_simulations)
            }

        # End db session.
        db.session.end()

        # Write response.
        utils_handler.write_json_response(self, data)
