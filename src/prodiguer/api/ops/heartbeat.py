# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.ops.heartbeat.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Operations heartbeat request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.api.utils import handler as handler_utils
from prodiguer.utils import rt



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
        handler_utils.write_response(self, error)


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
