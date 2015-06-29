# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.web.sim_metrics.fetch_setup.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Metric group setup fetch request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import utils
from prodiguer.web.sim_metrics import utils_validation as validator



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_GROUP = 'group'


class FetchSetupRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group fetch setup method request handler.

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
            # TODO validate headers
            if self.request.body:
                utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
            utils_handler.validate_request(self,
                query_validator=validator.validate_fetch_setup_query_arguments)

        def _decode_request():
            """Decodes request.

            """
            self.group = self.get_argument(_PARAM_GROUP)
            self.query = None if not self.request.body else \
                         utils.decode_json_payload(self, False)

        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'group': self.group,
                'columns': dao.fetch_columns(self.group),
                'data': dao.fetch_setup(self.group, self.query)
            }

        # Invoke tasks.
        utils_handler.invoke(self, _validate_request, [
            _decode_request,
            _set_output
        ])
