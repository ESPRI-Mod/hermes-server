# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.sim_metrics.rename.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group rename request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.request_validation import validator_sim_metrics as rv
from prodiguer.web.utils.http import HermesHTTPRequestHandler



# Query parameter names.
_PARAM_GROUP = 'group'


class SetHashesRequestHandler(HermesHTTPRequestHandler):
    """Simulation metric group set hashes method request handler.

    """
    def post(self):
        """HTTP POST handler.

        """
        def _do_work(self):
            """Sets the hash identifiers for all metrics within the group.

            """
            dao.set_hashes(self.get_argument(_PARAM_GROUP))

        # Invoke tasks.
        self.invoke(rv.validate_set_hashes, _do_work)
