# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_metrics.py
   :platform: Unix
   :synopsis: Set of metrics related data access operations.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
# Module imports.
import random

from . import constants, dao, session, types
from .types import (
    SimulationMetric,
    SimulationMetricGroup,
    )
from ..utils import runtime as rt


def get_group(name):
    """Returns a metric group.

    :param str name: Name of the metric group.

    :returns: A simulation metric group.
    :rtype: prodiguer.db.types.SimulationMetricGroup | None

    """
    return dao.get_by_name(SimulationMetricGroup, name)


def get_groups():
    """Returns a set of metric groups.

    :returns: A simulation metric group list.
    :rtype: list

    """
    return dao.get_all(SimulationMetricGroup)


def get_group_metrics(group_id):
    """Returns a set of metrics related to a group.

    :param str group_id: ID of a metric group.

    :returns: A collection of simulation metrics.
    :rtype: list

    """
    return dao.get_by_facet(SimulationMetric, 
                            qfilter=SimulationMetric.group_id==group_id,
                            get_iterable=True)


def get_group_metric_line_count(group_id):
    """Returns metric line count.

    :param str group_id: ID of a metric group.

    :returns: Count of number of lines within a metric.
    :rtype: int

    """
    return dao.get_count(SimulationMetric,
                         qfilter=SimulationMetric.group_id==group_id)


def delete_group(id):
    """Deletes a metric group.

    :param str id: ID of a metric group.

    """
    dao.delete_by_id(SimulationMetricGroup, id)


def delete_group_lines(id):
    """Deletes a set of metric lines by group.

    :param str id: ID of a metric group.

    """
    dao.delete_by_facet(SimulationMetric, SimulationMetric.group_id==id)


def delete_line(id):
    """Deletes a metric lines.

    :param str id: ID of a metric line.

    """
    dao.delete_by_id(SimulationMetric, id)
