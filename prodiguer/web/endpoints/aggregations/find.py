# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.aggregations.find.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: CMIP5 aggregation discovery upon local TDS IPSL-ESGF datanode or CICLAD filesystem..

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import json

from prodiguer.web.utils import ProdiguerHTTPRequestHandler



class FindRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation monitoring event request handler.

    """
    def get(self):
        """HTTP POST handler.

        """
        def _validate_request():
            """Validate request.

            """
            # TODO validate against discoverer
            pass


        def _find_aggregations():
            """Sets response to be returned to client.

            """
            # TODO invoke discoverer
            pass


        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                "msg": "TODO"
            }

        # Invoke tasks.
        self.invoke(_validate_request, [
            _find_aggregations,
            _set_output
        ])
