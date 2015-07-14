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
from prodiguer.web.request_validation import validator_sim_metrics as rv
from prodiguer.web.utils.http import ProdiguerHTTPRequestHandler



class FetchListRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation list metric request handler.

    """
    def get(self):
        """HTTP GET handler.

        """
        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'groups': dao.fetch_list()
            }

        def _set_headers():
            """Sets response headers to be returned to client.

            """
            self.set_header("Access-Control-Allow-Origin", "*")

        # Invoke tasks.
        self.invoke(rv.validate_fetch_list, [
            _set_output,
            _set_headers
            ])
