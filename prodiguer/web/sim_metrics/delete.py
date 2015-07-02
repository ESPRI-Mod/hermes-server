# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.sim_metrics.delete.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group delete request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import _utils as utils
from prodiguer.web.sim_metrics import _validator as validator



# Query parameter names.
_PARAM_GROUP = 'group'


class DeleteRequestHandler(utils_handler.ProdiguerWebServiceRequestHandler):
    """Simulation metric group delete method request handler.

    """
    def post(self):
        """HTTP POST handler.

        """
        def _do_work(self):
            """Deletes metrics from db.

            """
            query = None if not self.request.body else \
                    utils.decode_json_payload(self, False)

            dao.delete(self.get_argument(_PARAM_GROUP), query)

        # Invoke tasks.
        self.invoke(validator.validate_delete, _do_work)
