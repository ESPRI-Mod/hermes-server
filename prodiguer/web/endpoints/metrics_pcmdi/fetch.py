# -*- coding: utf-8 -*-
"""
.. module:: hermes.web.endpoints.metrics_pcmdi.fetch.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group fetch request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado
import voluptuous

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.request_validation import validator_metrics_pcmdi as rv
from prodiguer.web.utils.http import HermesHTTPRequestHandler



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_GROUP = 'group'


class FetchRequestHandler(HermesHTTPRequestHandler):
    """Simulation metric group fetch method request handler.

    """
    def get(self):
        """HTTP GET handler.

        """
        def _decode_request():
            """Decodes request.

            """
            self.group = self.get_argument(_PARAM_GROUP)
            self.query = self.decode_json_body(False)

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

        def _set_headers():
            """Sets response headers to be returned to client.

            """
            self.set_header("Access-Control-Allow-Origin", "*")

        # Invoke tasks.
        self.invoke(rv.validate_fetch, [
            _decode_request,
            _fetch_data,
            _format_data,
            _set_output,
            _set_headers
        ])
