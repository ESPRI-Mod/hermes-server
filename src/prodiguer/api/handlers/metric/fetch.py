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


# Default format query param value.
_DEFAULT_FORMAT = 'json'

# Default group.
_DEFAULT_GROUP = 'default'

# Metric id column name.
_COLUMN_METRIC_ID = "metric_id"

# Query parameter names.
_PARAM_GROUP = 'group'
_PARAM_FORMAT = 'format'


class FetchRequestHandler(utils.MetricWebRequestHandler):
    """Simulation metric group fetch method request handler.

    """
    def prepare(self):
        """Called at the beginning of request handling."""
        super(FetchRequestHandler, self).prepare()

        self.format = _DEFAULT_FORMAT
        self.group = None
        self.metrics = None


    def _validate_params(self):
        """Validates query params."""
        # ... group name
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))

        # ... format
        if _PARAM_FORMAT in self.request.arguments:
            utils.validate_format(self.get_argument(_PARAM_FORMAT))


    def _parse_params(self):
        """Parses query params."""
        # ... group name
        group_name = self.get_argument(_PARAM_GROUP)
        self.group = db.dao_metrics.get_group(group_name)
        if not self.group:
            raise ValueError("Group ({0}) unknown".format(group_name))

        # ... format
        if _PARAM_FORMAT in self.request.arguments:
            self.format = self.get_argument(_PARAM_FORMAT)


    def _set_metrics(self):
        """Loads metrics from db."""
        self.metrics = db.dao_metrics.get_group_metrics(self.group.id)


    def _set_output(self):
        """Sets response output."""
        def _get_metric(m):
            return json.loads(m.metric) + [m.id]

        self.output['group'] = self.group.name
        self.output['columns'] = json.loads(self.group.columns) + [_COLUMN_METRIC_ID]
        if self.metrics:
            self.output['metrics'] = [_get_metric(m) for m in self.metrics]


    def _set_output_csv(self):
        """Sets response csv output."""
        def get_value(value):
            return str(value)

        if self.format == 'csv':
            csv = ",".join(get_value(i) for i in self.output['columns'])
            for metric in self.output['metrics']:
                csv += '\n'
                csv += ",".join(get_value(i) for i in metric)
            self.output = csv


    def _write(self, error=None):
        """Write response output."""
        handler_utils.write(self, error, self.format)


    def _log(self, error=None):
        """Log execution."""
        handler_utils.log("metric", self, error)


    def get(self):
        # Define tasks.
        tasks = {
            "green": (
                self._validate_params,
                self._parse_params,
                self._set_metrics,
                self._set_output,
                self._set_output_csv,
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
