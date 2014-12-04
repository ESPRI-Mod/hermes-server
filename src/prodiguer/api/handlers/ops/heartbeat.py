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



class HeartbeatRequestHandler(tornado.web.RequestHandler):
    """Operations heartbeat request handler.

    """
    def prepare(self):
        """Called at the beginning of request handling."""
        self.output = {
            "status": 0
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
