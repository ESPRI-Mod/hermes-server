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

import tornado.web

# import ipsl_aggregation_discoverer as discoverer
from prodiguer.web import utils_handler



class FindRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring event request handler.

    """
    def _validate_request(self):
        """Validate HTTP POST request.

        """
        # TODO validate against discoverer
        pass


    def _find_aggregations(self):
        """Sets response to be returned to client.

        """
        # TODO invoke discoverer
        pass


    def _set_output(self):
        """Sets response to be returned to client.

        """
        self.output = {
            "msg": "TODO"
        }


    def post(self):
        """HTTP POST handler.

        """
        validation_tasks = [
            self._validate_request
        ]

        processing_tasks = [
            self._find_aggregations,
            self._set_output
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)

