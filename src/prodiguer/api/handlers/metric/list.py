# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.api.handlers.metric.list.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric list group request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""

# Module imports.
import json
import re

import tornado

from . import utils
from .. import utils as handler_utils
from .... import db
from .... utils import (
    config as cfg,
    runtime as rt
    )



class ListRequestHandler(tornado.web.RequestHandler):
    """Simulation list metric request handler.

    """
    def set_default_headers(self):
        """Set HTTP headers at the beginning of the request."""
        self.set_header(utils.HTTP_HEADER_Access_Control_Allow_Origin,
                        ",".join(cfg.api.metric.cors_white_list))


    def prepare(self):
        """Called at the beginning of request handling."""
        self.output = {
            'groups': []
        }


    def _set_group(self):
        """Loads groups from db."""
        self.groups = db.dao_metrics.get_groups()


    def _set_output(self):
        """Sets response data."""
        if self.groups:
            self.output['groups'] = [i.name for i in self.groups]


    def _write(self, error=None):
        """Write response output."""
        handler_utils.write(self, error)


    def _log(self, error=None):
        """Log execution."""
        handler_utils.log("metric", self, error)


    def get(self):
        # Define tasks.
        tasks = {
            "green": (
                self._set_group,
                self._set_output,
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
