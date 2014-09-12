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
from ....utils import (
    convert,
    runtime as rt,
    )


# Query parameter names.
_PARAM_GROUP = 'group'


class DeleteLinesRequestHandler(utils.MetricWebRequestHandler):
    """Simulation metric delete lines method request handler.

    """
    def _validate_headers(self):
        """Validates request headers."""
        # Verify json data type.
        if 'Content-Type' not in self.request.headers:
            raise ValueError("Content-Type is undefined")
        if self.request.headers['Content-Type'] != 'application/json':
            raise ValueError("Content-Type must be application/json")


    def _decode_body(self):
        """Decodes request body."""
        # Load json.
        data = json.loads(self.request.body)

        # Convert to namedtuple.
        self.data = convert.dict_to_namedtuple(data)


    def _validate_body(self):
        """Validates request body."""
        # Validate fields.
        for fname, ftype in [
            ('metric_id_list', list),
            ]:
            if fname not in self.data._fields:
                raise KeyError("Undefined field: {0}".format(fname))
            if not isinstance(getattr(self.data, fname), ftype):
                raise ValueError("Invalid field type: {0}".format(fname))


    def _delete_metric_lines(self):
        """Deletes metric lines."""
        for metric_id in self.data.metric_id_list:
            db.dao_metrics.delete_line(metric_id)


    def _commit_to_db(self):
        """Commits db changes."""
        db.session.commit()


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
                self._validate_headers,
                self._decode_body,
                self._validate_body,
                self._delete_metric_lines,
                self._commit_to_db,
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
