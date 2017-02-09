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

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.request_validation import validator_metrics_pcmdi as rv
from prodiguer.web.utils.http import HermesHTTPRequestHandler



# Query parameter names.
_PARAM_GROUP = 'group'


class FetchColumnsRequestHandler(HermesHTTPRequestHandler):
    """Simulation metric group fetch columns method request handler.

    """
    def get(self):
        """HTTP GET handler.

        """
        def _decode_request():
            """Decodes request.

            """
            self.group = self.get_argument(_PARAM_GROUP)

        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'group': self.group,
                'columns': dao.fetch_columns(self.group)
            }

        def _set_headers():
            """Sets response headers to be returned to client.

            """
            self.set_header("Access-Control-Allow-Origin", "*")


        def _cleanup():
            """Performs cleanup after request processing.

            """
            del self.group


        # Invoke tasks.
        self.invoke(rv.validate_fetch_columns, [
            _decode_request,
            _set_output,
            _set_headers,
            _cleanup
        ])
