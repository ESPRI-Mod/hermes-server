# -*- coding: utf-8 -*-
"""
.. module:: prodiguer.api.handlers.metric.fetch_setup.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Metric group setup fetch request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import json

from . import utils
from .. import utils as handler_utils
from .... import db
from .... utils import rt



# Default group.
_DEFAULT_GROUP = 'default'

# Query parameter names.
_PARAM_GROUP = 'group'


class FetchSetupRequestHandler(utils.MetricWebRequestHandler):
    """Simulation metric group fetch setup method request handler.

    """
    def prepare(self):
        """Called at the beginning of request handling."""
        super(FetchSetupRequestHandler, self).prepare()

        self.group = None
        self.metrics = None
        self.setup_data = []


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


    def _set_metrics(self):
        """Loads metrics from db."""
        self.metrics = db.dao_metrics.get_group_metrics(self.group.id)
        self.metrics = [json.loads(m.metric) for m in self.metrics]


    def _set_setup(self):
        """Loads metrics from db."""
        if not self.metrics:
            return

        for i in range(len(self.metrics[0])):
            self.setup_data.append(sorted(set([m[i] for m in self.metrics])))


    def _set_output(self):
        """Sets response output."""
        self.output['columns'] = json.loads(self.group.columns)
        self.output['group'] = self.group.name
        self.output['data'] = self.setup_data


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
                self._set_metrics,
                self._set_setup,
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
