# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.sim_metrics.add.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group add request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from collections import OrderedDict

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.endpoints.sim_metrics import request_validator
from prodiguer.web.utils import ProdiguerHTTPRequestHandler



# Query parameter names.
_PARAM_DUPLICATE_ACTION = 'duplicate_action'



class AddRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation metric group add method request handler.

    """
    def post(self):
        """HTTP POST handler.

        """
        def _decode_request():
            """Decodes request.

            """
            self.duplicate_action = self.get_argument(_PARAM_DUPLICATE_ACTION, 'skip')
            self.payload = self.decode_json_body()


        def _insert_metrics():
            """Inserts metrics to the db.

            """
            def _format(metric):
                """Formats a metric for insertion into db."""
                return [(c, metric[i]) for i, c in enumerate(self.payload.columns)]

            def _format_metrics():
                """Returns list of formatted metrics."""
                return [OrderedDict(_format(m)) for m in self.payload.metrics]

            self.added, self.duplicates = \
                dao.add(self.payload.group, _format_metrics(), self.duplicate_action)


        def _set_output():
                """Sets response to be returned to client.

                """
                self.output = {
                    'group': self.payload.group,
                    'added_count': len(self.added),
                    'duplicate_count': len(self.duplicates)
                }

        # Invoke tasks.
        self.invoke(request_validator.validate_add, [
            _decode_request,
            _insert_metrics,
            _set_output
        ])

