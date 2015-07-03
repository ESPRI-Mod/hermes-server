# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.sim_metrics.rename.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group rename request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.endpoints.sim_metrics import request_validator
from prodiguer.web.utils import ProdiguerHTTPRequestHandler



# Query parameter names.
_PARAM_GROUP = 'group'
_PARAM_NEW_NAME = 'new_name'


class RenameRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation metric group rename method request handler.

    """
    def post(self):
    	"""HTTP POST handler.

    	"""
        def _decode_request():
            """Decodes request.

            """
            self.group = self.get_argument(_PARAM_GROUP)
            self.new_name = self.get_argument(_PARAM_NEW_NAME)

        def _rename_metric_group():
            """Renames metrics group within db.

            """
            dao.rename(self.group, self.new_name)

        # Invoke tasks.
        self.invoke(request_validator.validate_rename, [
            _decode_request,
            _rename_metric_group,
        ])
