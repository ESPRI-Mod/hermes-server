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
            utils_handler.validate_vanilla_request
        ]

        processing_tasks = [
            self._set_output
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)
