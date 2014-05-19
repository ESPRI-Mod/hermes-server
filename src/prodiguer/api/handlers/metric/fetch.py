# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.api.handlers.metric.fetch.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group fetch request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""

# Module imports.
import csv
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


# Default format query param value.
_DEFAULT_FORMAT = 'json'

# Default group.
_DEFAULT_GROUP = 'default'

# Default headersonly query param value.
_DEFAULT_HEADERSONLY = False

# Metric id column name.
_COLUMN_METRIC_ID = "metric_id"

# Query parameter names.
_PARAM_GROUP = 'group'
_PARAM_FORMAT = 'format'
_PARAM_HEADERSONLY = 'headersonly'


class FetchRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group fetch method request handler.

    """
    def set_default_headers(self):
        """Set HTTP headers at the beginning of the request."""
        self.set_header(utils.HTTP_HEADER_Access_Control_Allow_Origin,
                        ",".join(cfg.api.metric.cors_white_list))


    def prepare(self):
        """Called at the beginning of request handling."""
        self.format = _DEFAULT_FORMAT
        self.headersonly = _DEFAULT_HEADERSONLY
        self.group = None
        self.metrics = None
        self.output = {}


    def _validate_params(self):
        """Validates query params."""
        # ... group name
        if _PARAM_GROUP in self.request.arguments:
            utils.validate_group_name(self.get_argument(_PARAM_GROUP))

        # ... format
        if _PARAM_FORMAT in self.request.arguments:
            utils.validate_format(self.get_argument(_PARAM_FORMAT))

        # ... headersonly
        if _PARAM_HEADERSONLY in self.request.arguments:
            if self.get_argument(_PARAM_HEADERSONLY) not in ['true', 'false']:
                raise ValueError("Invalid headersonly query parameter value")


    def _parse_params(self):
        """Parses query params."""
        # ... group name
        if _PARAM_GROUP in self.request.arguments:
            group_name = self.get_argument(_PARAM_GROUP)
        else:
            group_name = _DEFAULT_GROUP
        self.group = db.dao_metrics.get_group(group_name)
        if not self.group:
            raise ValueError("Group ({0}) unknown".format(group_name))

        # ... format
        if _PARAM_FORMAT in self.request.arguments:
            self.format = self.get_argument(_PARAM_FORMAT)

        # ... headersonly
        if _PARAM_HEADERSONLY in self.request.arguments and \
           self.get_argument(_PARAM_HEADERSONLY) == "true":
            self.headersonly = True


    def _set_metrics(self):
        """Loads metrics from db."""
        if not self.headersonly and self.group:
            self.metrics = db.dao_metrics.get_group_metrics(self.group.id)


    def _set_output(self):
        """Sets response output."""
        def _get_metric(m):
            return json.loads(m.metric) + [m.id]

        self.output['group'] = self.group.name
        self.output['columns'] = json.loads(self.group.columns) + [_COLUMN_METRIC_ID]
        if self.metrics:
            self.output['metrics'] = map(_get_metric, self.metrics)


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
