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

from prodiguer.utils import rt
from prodiguer.web import utils_handler
from prodiguer.db import pgres as db



def _get_data(func):
    """Returns data for front-end.

    """
    return db.utils.get_collection(func())


class FrontEndSetupAllRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring all simulations front end setup request handler.

    """
    def _validate_request(self):
        """Validate HTTP GET request.

        """
        # Invalid if request has associated query, body or files.
        if not utils_handler.is_vanilla_request(self):
            raise tornado.httputil.HTTPInputError()


    def _set_output(self):
        """Sets response to be returned to client.

        """
        db.session.start()
        self.output = {
            'job_history':
                _get_data(db.dao_monitoring.retrieve_active_jobs),
            'simulation_list':
                _get_data(db.dao_monitoring.retrieve_active_simulations)
        }
        db.session.end()


    def get(self, *args):
        """HTTP GET handler.

        """
        validation_tasks = [
            self._validate_request
        ]

        processing_tasks = [
            self._set_output
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)
