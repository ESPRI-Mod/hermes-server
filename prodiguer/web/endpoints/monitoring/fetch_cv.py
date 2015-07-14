# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.monitoring.setup.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db import pgres as db
from prodiguer.web.request_validation import validator_monitoring as rv
from prodiguer.web.utils.http import ProdiguerHTTPRequestHandler



class FetchControlledVocabularyRequestHandler(ProdiguerHTTPRequestHandler):
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
        self.invoke(rv.validate_fetch_cv, _set_output)
