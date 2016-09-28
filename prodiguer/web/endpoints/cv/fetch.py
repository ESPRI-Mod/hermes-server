# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.cv.fetch.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Fetch cv terms request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao
from prodiguer.utils import logger
from prodiguer.web.utils.http1 import process_request



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


class FetchRequestHandler(tornado.web.RequestHandler):
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
                self.cv_terms = [(t.display_name, t.name, t.sort_key, t.synonyms or [], t.typeof, t.uid)
                                 for t in dao.get_all(db.types.ControlledVocabularyTerm)
                                 if t.typeof not in _EXCLUDED_TERMSETS]


        def _set_output():
            """Sets response to be returned to client.

            """
            self.write_raw_output = True
            self.output = {
                'cvTerms': self.cv_terms
            }


        def _cleanup():
            """Performs cleanup after request processing.

            """
            del self.cv_terms


        # Process request.
        process_request(self, [
            _set_data,
            _set_output,
            _cleanup
            ])
