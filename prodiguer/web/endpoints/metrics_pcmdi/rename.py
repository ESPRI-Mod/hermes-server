# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.metrics_pcmdi.rename.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group rename request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from hermes.db.mongo import dao_metrics as dao
from hermes.web.request_validation import validator_metrics_pcmdi as rv
from hermes.web.utils.http import HermesHTTPRequestHandler



# Query parameter names.
_PARAM_GROUP = 'group'
_PARAM_NEW_NAME = 'new_name'


class RenameRequestHandler(HermesHTTPRequestHandler):
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

        def _cleanup():
            """Performs cleanup after request processing.

            """
            del self.group
            del self.new_name

        # Invoke tasks.
        self.invoke(rv.validate_rename, [
            _decode_request,
            _rename_metric_group,
            _cleanup
        ])
