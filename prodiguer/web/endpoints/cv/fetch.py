# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.cv.fetch.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Fetch cv terms request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao
from prodiguer.utils import logger
from prodiguer.web.request_validation import validator_cv as rv
from prodiguer.web.utils.http import HermesHTTPRequestHandler



# Set of termsets not required for front-end.
_EXCLUDED_TERMSETS = {
    'compute_node',
    'execution_state',
    'experiment_group',
    'institute',
    'job_type',
    'message_application',
    'message_producer',
    'message_type',
    'message_user',
    'model_forcing'
    }


def _map_term(term):
    """Maps a term for output to client.

    """
    return (term.display_name, term.name, term.sort_key, term.synonyms or [], term.typeof, term.uid)


class FetchRequestHandler(HermesHTTPRequestHandler):
    """Fetch controlled vocabulary setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _set_data():
            """Pulls data from db.

            """
            with db.session.create():
                logger.log_web("[{}]: executing db query: retrieve_cv_terms".format(id(self)))
                self.cv_terms = [_map_term(t) for t in dao.get_all(db.types.ControlledVocabularyTerm)
                                if t.typeof not in _EXCLUDED_TERMSETS]


        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'cvTerms': self.cv_terms
            }


        # Invoke tasks.
        self.invoke(rv.validate_fetch, [
            _set_data,
            _set_output
        ], write_raw_output=True)
