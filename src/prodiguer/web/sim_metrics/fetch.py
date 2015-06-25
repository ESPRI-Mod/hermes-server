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
from voluptuous import All, Invalid, Schema, Required

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.utils import rt
from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import utils



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_GROUP = 'group'


def _validate_get_request_query_arguments(handler):
    """Validates GET endpoint HTTP query arguments.

    """
    def GroupID():
        """Validates incoming group-id query parameter.

        """
        def f(val):
            """Inner function.

            """
            utils.validate_group_name(val[0])

        return f


    def Sequence(expected_type, expected_length=1):
        """Validates a sequence of query parameter values.

        """
        def f(val):
            """Inner function.

            """
            # Validate sequence length.
            if len(val) != expected_length:
                raise ValueError("Invalid request")

            # Validate sequence type.
            for item in val:
                try:
                    expected_type(item)
                except ValueError:
                    raise ValueError("Invalid request")

            return val

        return f


    # Set query argument validation schema.
    schema = Schema({
        Required(_PARAM_GROUP): All(list, Sequence(str), GroupID())
    })

    # Apply query argument validation.
    schema(handler.request.query_arguments)


class FetchRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group fetch method request handler.

    """
    def set_default_headers(self):
        """Set default HTTP response headers.

        """
        utils.set_cors_white_list(self)


    def _validate_request(self):
        """Validate HTTP GET request.

        """
        if self.request.body:
            utils.validate_http_content_type(self, _CONTENT_TYPE_JSON)
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))


    def _decode_request(self):
        """Decodes request.

        """
        self.group = self.get_argument(_PARAM_GROUP)
        if self.request.body:
            self.query = utils.decode_json_payload(self, False)
        else:
            self.query = None


    def _fetch_data(self):
        """Fetches data from db.

        """
        self.columns = dao.fetch_columns(self.group, True)
        self.metrics = dao.fetch(self.group, self.query)


    def _format_data(self):
        """Formats data.

        """
        # MongoDb appends the _id column to the beginning of each metric sets,
        # however we want it to be appended to the end of each metric set.
        self.metrics = [m[1:] + [m[0]] for m in
                        [m.values() for m in self.metrics]]


    def _set_output(self):
        """Sets response to be returned to client.

        """
        self.output = {
            'group': self.group,
            'columns': self.columns,
            'metrics': self.metrics
        }


    def get(self):
        """HTTP GET handler.

        """
        def _validate_request():
            """Request validator.

            """
            utils_handler.validate_request(self,
                query_validator=_validate_get_request_query_arguments)

        validation_tasks = [
            _validate_request
        ]

        processing_tasks = [
            self._decode_request,
            self._fetch_data,
            self._format_data,
            self._set_output,
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)
