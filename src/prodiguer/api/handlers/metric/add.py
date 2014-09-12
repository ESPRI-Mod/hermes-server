# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.metric.add.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group add request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import json

import tornado

from . import utils
from .. import utils as handler_utils
from .... import db
from .... utils import config, convert, rt



# Default group.
_DEFAULT_GROUP = 'default'

# Metric id column name.
_COLUMN_METRIC_ID = "metric_id"

# Set of supported contnet types.
_CONTENT_TYPE_CSV = "text/csv"
_CONTENT_TYPE_JSON = "application/json"
_CONTENT_TYPES = (_CONTENT_TYPE_JSON, )


class AddRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group add method request handler.

    """
    def prepare(self):
        """Called at the beginning of request handling."""
        # Start session.
        db.session.start(config.db.connections.main)

        # Initialise output.
        self.output = {}


    def _validate_headers(self):
        """Validates request headers."""
        # Verify json data type.
        if 'Content-Type' not in self.request.headers:
            raise ValueError("Content-Type is undefined")
        if self.request.headers['Content-Type'] not in _CONTENT_TYPES:
            raise ValueError("Unsupported content-type")


    def _decode_body_json(self):
        """Decodes request body in json format."""
        # Escape if metrics not psoted as json.
        if self.request.headers['Content-Type'] != _CONTENT_TYPE_JSON:
            return

        # Load json.
        data = json.loads(self.request.body)

        # Set default group if necessary.
        if 'group' not in data:
            data['group'] = _DEFAULT_GROUP

        # Convert to namedtuple.
        self.data = convert.dict_to_namedtuple(data)


    def _validate_body(self):
        """Validates request body."""
        # Validate fields.
        for fname, ftype in [
            ('group', unicode),
            ('columns', list),
            ('metrics', list),
            ]:
            if fname not in self.data._fields:
                raise KeyError("Undefined field: {0}".format(fname))
            if not isinstance(getattr(self.data, fname), ftype):
                raise ValueError("Invalid field type: {0}".format(fname))

        # Validate group name.
        utils.validate_group_name(self.data.group)

        # Validate that metric_id is not a column.
        if _COLUMN_METRIC_ID in self.data.columns:
            raise KeyError("metric_id is a reserved column name")

        # Validate that length   of each metric is same as length of group columns.
        for metric in self.data.metrics:
            if len(metric) != len(self.data.columns):
                raise ValueError("Invalid metric: number of columns does not match group columns")


    def _set_metrics_group(self):
        """Persists metrics group to the db."""
        # Retrieve group.
        self.group = db.dao_metrics.get_group(self.data.group)

        # Insert if not found.
        if self.group is None:
            smg = self.group = db.types.SimulationMetricGroup()
            smg.name = self.data.group
            smg.columns = json.dumps(self.data.columns)
            db.session.add(smg)


    def _set_metrics(self):
        """Persists metrics to the db."""
        # Iterate metrics & persist.
        for metric in self.data.metrics:
            sm = db.types.SimulationMetric()
            sm.group_id = self.group.id
            sm.metric = json.dumps(metric)
            db.session.add(sm)


    def _write(self, error=None):
        """Write response output."""
        self.output['group'] = self.data.group
        handler_utils.write(self, error)


    def _log(self, error=None):
        """Log execution."""
        handler_utils.log("metric", self, error)


    def post(self):
        # Define tasks.
        tasks = {
            "green": (
                self._validate_headers,
                self._decode_body_json,
                self._validate_body,
                self._set_metrics_group,
                self._set_metrics,
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
