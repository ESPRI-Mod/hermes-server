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
from prodiguer.web.request_validation import validator_cv as rv
from prodiguer.web.utils.http import ProdiguerHTTPRequestHandler



class FetchRequestHandler(ProdiguerHTTPRequestHandler):
    """Fetch controlled vocabulary setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _get_terms():
            """Returns sorted list of cv terms.

            """
            data  = db.utils.get_list(db.types.ControlledVocabularyTerm)

            for item in data:
                if item['sort_key'] is None:
                    del item['sort_key']
                if item['synonyms'] is None:
                    del item['synonyms']

            return data


        def _set_output():
            """Sets response to be returned to client.

            """
            db.session.start()
            self.output = {
                'cv_terms': _get_terms()
            }
            db.session.end()

        # Invoke tasks.
        self.invoke(rv.validate_fetch, _set_output)