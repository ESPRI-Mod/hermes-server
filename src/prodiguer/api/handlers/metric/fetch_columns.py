# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.api.handlers.metric.fetch.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group fetch request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import json

from . import utils
from .. import utils as handler_utils
from .... import db
from .... utils import rt



# Metric id column name.
_COLUMN_METRIC_ID = "metric_id"

# Query parameter names.
_PARAM_GROUP = 'group'


class FetchColumnsRequestHandler(utils.MetricWebRequestHandler):
    """Simulation metric group fetch columns method request handler.

    """
    def prepare(self):
        """Called at the beginning of request handling."""
        super(FetchColumnsRequestHandler, self).prepare()

        self.group = None


    def _validate_params(self):
        """Validates query params."""
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))


    def _parse_params(self):
        """Parses query params."""
        group_name = self.get_argument(_PARAM_GROUP)
        self.group = db.dao_metrics.get_group(group_name)
        if not self.group:
            raise ValueError("Group ({0}) unknown".format(group_name))


    def _set_output(self):
        """Sets response output."""
        self.output['group'] = self.group.name
        self.output['columns'] = json.loads(self.group.columns) + [_COLUMN_METRIC_ID]


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
                self._validate_params,
                self._parse_params,
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
