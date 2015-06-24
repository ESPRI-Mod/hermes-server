# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.ops.heartbeat.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Operations heartbeat request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.web import utils_handler
from prodiguer.utils import rt



class ListEndpointsRequestHandler(tornado.web.RequestHandler):
    """Operations list endpoints request handler.

    """
    def _set_output(self):
        """Sets response to be returned to client.

        """
        self.output = {
            'endpoints': [
                r'/api/1/monitoring/fe/cv',
                r'/api/1/monitoring/fe/setup/all',
                r'/api/1/monitoring/fe/setup/one',
                r'/api/1/monitoring/fe/ws/all',
                r'/api/1/monitoring/event',
                r'/api/1/metric/add',
                r'/api/1/metric/delete',
                r'/api/1/metric/fetch',
                r'/api/1/metric/fetch_count',
                r'/api/1/metric/fetch_columns',
                r'/api/1/metric/fetch_list',
                r'/api/1/metric/fetch_setup',
                r'/api/1/metric/rename',
                r'/api/1/metric/set_hashes',
                r'/api/1/ops/heartbeat',
                r'/api/1/ops/endpoints'
                ]
            }


    def get(self):
        """HTTP GET handler.

        """
        validation_tasks = [
            utils_handler.validate_vanilla_request
        ]

        processing_tasks = [
            self._set_output
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)
