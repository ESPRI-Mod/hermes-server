# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.cv.fetch.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Fetch cv terms request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db import pgres as db
from prodiguer.db. pgres import dao
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


def map_term(term):
    """Maps a term for output to client.

    """
    return (term.display_name, term.name, term.sort_key, term.synonyms or [], term.typeof, term.uid)


class FetchRequestHandler(ProdiguerHTTPRequestHandler):
    """Fetch controlled vocabulary setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _get_terms():
            """Returns sorted list of cv terms.

            """
            return [map_term(t) for t in dao.get_all(db.types.ControlledVocabularyTerm)
                    if t.typeof not in _EXCLUDED_TERMSETS]


        def _set_output():
            """Sets response to be returned to client.

            """
            db.session.start()
            try:
                self.output = {
                    'cvTerms': _get_terms()
                }
            finally:
                db.session.end()

        # Invoke tasks.
        self.invoke(rv.validate_fetch, _set_output, write_raw_output=True)
