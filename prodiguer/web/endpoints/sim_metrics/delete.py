# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.sim_metrics.delete.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group delete request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.endpoints.sim_metrics import request_validator
from prodiguer.web.utils import ProdiguerHTTPRequestHandler



# Query parameter names.
_PARAM_GROUP = 'group'


class DeleteRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation metric group delete method request handler.

    """
    def post(self):
        """HTTP POST handler.

        """
        def _do_work():
            """Deletes metrics from db.

            """
            query = self.decode_json_body(False)

            dao.delete(self.get_argument(_PARAM_GROUP), query)

        # Invoke tasks.
        self.invoke(request_validator.validate_delete, _do_work)
