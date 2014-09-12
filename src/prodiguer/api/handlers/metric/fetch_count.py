# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.api.handlers.metric.fetch_line_count.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Metric group line count request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
from . import utils
from .. import utils as handler_utils
from .... import db
from .... utils import rt



# Default group.
_DEFAULT_GROUP = 'default'

# Query parameter names.
_PARAM_GROUP = 'group'


class FetchCountRequestHandler(utils.MetricWebRequestHandler):
    """Simulation metric group fetch line count method request handler.

    """
    def prepare(self):
        """Called at the beginning of request handling."""
        super(FetchCountRequestHandler, self).prepare()

        self.group = None
        self.line_count = 0


    def _validate_params(self):
        """Validates query params."""
        # ... group
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))


    def _parse_params(self):
        """Parses query params."""
        # ... group
        group_name = self.get_argument(_PARAM_GROUP)
        group = db.dao_metrics.get_group(group_name)
        if group:
            self.group = group
        else:
            raise ValueError("Unknown metric group: {0}".format(group_name))


    def _set_line_count(self):
        """Calculates metric line count via a db query."""
        self.line_count = db.dao_metrics.get_group_metric_line_count(self.group.id)


    def _set_output(self):
        """Sets response output."""
        self.output['group'] = self.group.name
        self.output['count'] = self.line_count


    def _write(self, error=None):
        """Write response output."""
        handler_utils.write(self, error)


    def _log(self, error=None):
        """Log execution."""
        handler_utils.log("metric", self, error)


    def get(self):
        """HTTP GET request handler.

        """
        # Define tasks.
        tasks = {
            "green": (
                self._validate_params,
                self._parse_params,
                self._set_line_count,
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
