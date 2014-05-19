# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.metric.delete.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group delete request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import json
import re

import tornado

from . import utils
from .. import utils as handler_utils
from .... import db
from ....utils import runtime as rt



# Query parameter names.
_PARAM_GROUP = 'group'


class DeleteGroupRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group delete method request handler.

    """
    def prepare(self):
        """Called at the beginning of request handling."""
        self.output = {}


    def _validate_params(self):
        """Validates query params."""
        if 'group' in self.request.arguments:
            utils.validate_group_name(self.get_argument('group'))


    def _set_group(self):
        """Loads group from db prior to deletion."""
        group_name = self.get_argument(_PARAM_GROUP) if _PARAM_GROUP in self.request.arguments else 'default'
        self.group = db.dao_metrics.get_group(group_name)


    def _delete_lines(self):
        """Deletes group metric lines."""
        if self.group:
            db.dao_metrics.delete_group_lines(self.group.id)


    def _delete_group(self):
        """Deletes group."""
        if self.group:
            db.dao_metrics.delete_group(self.group.id)


    def _write(self, error=None):
        """Write response output."""
        handler_utils.write(self, error)


    def _log(self, error=None):
        """Log execution."""
        handler_utils.log("metric", self, error)


    def post(self):
        # Define tasks.
        tasks = {
            "green": (
                self._validate_params, 
                self._set_group,
                self._delete_lines,
                self._delete_group,                
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
