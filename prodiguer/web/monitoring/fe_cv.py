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



class FrontEndControlledVocabularyRequestHandler(utils_handler.ProdiguerWebServiceRequestHandler):
    """Simulation monitoring front end controlled vocabulary setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _set_output():
            """Sets response to be returned to client.

            """
            db.session.start()
            self.output = {
                'cv_terms':
                    db.utils.get_list(db.types.ControlledVocabularyTerm)
            }
            db.session.end()

        # Invoke tasks.
        self.invoke(utils_handler.validate_vanilla_request, _set_output)
