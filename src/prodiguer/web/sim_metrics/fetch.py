# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.web.sim_metrics.fetch.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group fetch request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado
import voluptuous

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import utils
from prodiguer.web.sim_metrics import utils_validation as validator



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_GROUP = 'group'



class FetchRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group fetch method request handler.

    """
    def set_default_headers(self):
        """Set default HTTP response headers.

        """
        utils.set_cors_white_list(self)


    def get(self):
        """HTTP GET handler.

        """
        def _validate_request():
            """Request validator.

            """
            utils_handler.validate_request(self,
                query_validator=validator.validate_fetch_query_arguments)

        def _decode_request():
            """Decodes request.

            """
            self.group = self.get_argument(_PARAM_GROUP)
            self.query = None if not self.request.body else \
                         utils.decode_json_payload(self, False)

        def _fetch_data():
            """Fetches data from db.

            """
            self.columns = dao.fetch_columns(self.group)
            self.metrics = dao.fetch(self.group, self.query)

        def _format_data():
            """Formats data.

            """
            # Move _id column to the end of each metric set.
            self.metrics = [m[1:] + [m[0]] for m in
                            [m.values() for m in self.metrics]]

        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'group': self.group,
                'columns': self.columns,
                'metrics': self.metrics
            }

        # Invoke tasks.
        utils_handler.invoke(self, _validate_request, [
            _decode_request,
            _fetch_data,
            _format_data,
            _set_output,
        ])
