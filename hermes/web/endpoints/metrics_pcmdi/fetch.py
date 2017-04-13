# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.monitoring.fetch_all.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from hermes.db.mongo import dao_metrics as dao
from hermes.web.utils.http1 import process_request
from hermes.web.utils.http1 import decode_json_payload



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_GROUP = 'group'




class FetchRequestHandler(tornado.web.RequestHandler):
    """Fetches a metrics group.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _decode_request():
            """Decodes request.

            """
            self.group = self.get_argument(_PARAM_GROUP)
            self.query = decode_json_payload(self, False)


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


        def _cleanup():
            """Performs cleanup after request processing.

            """
            del self.group
            del self.query
            del self.columns
            del self.metrics


        # Process request.
        process_request(self, [
            _decode_request,
            _fetch_data,
            _format_data,
            _set_output,
            _set_headers,
            _cleanup
            ])
