# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.ops.heartbeat.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Operations heartbeat request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from ....utils import runtime as rt
from .. import utils as handler_utils



class ListEndpointsRequestHandler(tornado.web.RequestHandler):
    """Operations list endpoints request handler.

    """
    def prepare(self):
        """Called at the beginning of request handling."""
        self.output = {
            "monitoring": [
                r'/api/1/monitoring/fe/setup',
                r'/api/1/monitoring/fe/ws',
                r'/api/1/monitoring/event',
            ],
            "metric": [
                r'/api/1/metric/add',
                r'/api/1/metric/delete',
                r'/api/1/metric/fetch',
                r'/api/1/metric/fetch_headers',
                r'/api/1/metric/fetch_setup',
                r'/api/1/metric/fetch_count',
                r'/api/1/metric/list'
            ],
            "ops": [
                r'/api/1/ops/heartbeat',
                r'/api/1/ops/endpoints'
            ]
        }


    def _write(self, error=None):
        """Write response output."""
        handler_utils.write(self, error)


    def _log(self, error=None):
        """Logs execution."""
        handler_utils.log("ops", self, error)


    def get(self):
        # Define tasks.
        tasks = {
            "green": (
                self._write,
                self._log,
                ),
            "red": (
                self._write,
                self._log,
                )
        }

        # Invoke tasks.
        rt.invoke(tasks)
