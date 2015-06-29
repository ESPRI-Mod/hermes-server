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

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import utils
from prodiguer.web.sim_metrics import utils_validation as validator



# Query parameter names.
_PARAM_GROUP = 'group'


class FetchColumnsRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group fetch columns method request handler.

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
                query_validator=validator.validate_fetch_columns_query_arguments)

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

        # Invoke tasks.
        utils_handler.invoke(self, _validate_request, [
            _decode_request,
            _set_output,
        ])
