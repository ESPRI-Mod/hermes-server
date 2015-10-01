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



# Set of termsets not required for front-end.
_EXCLUDED_TERMSETS = {
    'execution_state',
    'institute',
    'message_type',
    'message_application',
    'message_producer',
    'message_user',
    'model_forcing',
    'job_type',
    'experiment_group'
    }

# Set of fields that can be stripped from payload if they are null.
_NULLABLE_FIELDS = {
    'sort_key',
    'synonyms'
}



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
            data = [i for i in data if i['typeof'] not in _EXCLUDED_TERMSETS]
            for item in data:
                for field in _NULLABLE_FIELDS:
                    if item[field] is None:
                        del item[field]

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
