# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.web.endpoints.sim_metrics.list.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric list group request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web.endpoints.sim_metrics import utils
from prodiguer.web.endpoints.sim_metrics import _validator as validator
from prodiguer.web.utils import ProdiguerHTTPRequestHandler



class FetchListRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation list metric request handler.

    """
    def set_default_headers(self):
        """Set default HTTP response headers.

        """
        utils.set_cors_white_list(self)


    def get(self):
        """HTTP GET handler.

        """
        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'groups': dao.fetch_list()
            }

        # Invoke tasks.
        self.invoke(validator.validate_fetch_list, _set_output)
