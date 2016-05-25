# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.web.endpoints.sim_metrics.fetch_setup.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Metric group setup fetch request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.request_validation import validator_sim_metrics as rv
from prodiguer.web.utils.http import HermesHTTPRequestHandler



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_GROUP = 'group'


class FetchSetupRequestHandler(HermesHTTPRequestHandler):
    """Simulation metric group fetch setup method request handler.

    """
    def get(self):
        """HTTP GET handler.

        """
        def _decode_request():
            """Decodes request.

            """
            self.group = self.get_argument(_PARAM_GROUP)
            self.query = self.decode_json_body(False)

        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'group': self.group,
                'columns': dao.fetch_columns(self.group, True),
                'data': dao.fetch_setup(self.group, self.query)
            }

        def _set_headers():
            """Sets response headers to be returned to client.

            """
            self.set_header("Access-Control-Allow-Origin", "*")

        # Invoke tasks.
        self.invoke(rv.validate_fetch_setup, [
            _decode_request,
            _set_output,
            _set_headers
        ])
