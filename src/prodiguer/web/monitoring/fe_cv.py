# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.monitoring.setup.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.httputil
import tornado.web

from prodiguer.web import utils_handler
from prodiguer.db import pgres as db



class FrontEndControlledVocabularyRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring front end controlled vocabulary setup request handler.

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
            'cv_terms':
                db.utils.get_list(db.types.ControlledVocabularyTerm)
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
