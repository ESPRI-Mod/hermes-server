# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.dao_monitoring.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import distinct

from hermes.db.pgres import dao
from hermes.db.pgres import session
from hermes.db.pgres import types
from hermes.db.pgres import validator_dao_monitoring as validator
from hermes.db.pgres.dao_monitoring_job import *
from hermes.db.pgres.dao_monitoring_simulation import *
from hermes.utils import decorators



@decorators.validate(validator.validate_exists)
def exists(uid):
    """Returns a flag indicating whether simulation already exists.

    :param str uid: UID of simulation.

    :returns: True if simulation exists false otherwise.
    :rtype: bool

    """
    qfilter = types.Simulation.uid == unicode(uid)

    return dao.get_count(types.Simulation, qfilter=qfilter) == 1


@decorators.validate(validator.validate_persist_environment_metric)
def persist_environment_metric(
    action_name,
    action_timestamp,
    dir_from,
    dir_to,
    duration_ms,
    job_uid,
    simulation_uid,
    size_mb,
    throughput_mb_s
    ):
    """Persists environment metric information to db.

    :param str action_name: Name of libIGCM action.
    :param str dir_from: Directory from which data was copied.
    :param str dir_to: Directory to which data was copied.
    :param integer duration_ms: Duration in milliseconds of action.
    :param str job_uid: Job UID.
    :param str simulation_uid: Simulation UID.
    :param float size_mb: Size in megabytes of moved file(s).
    :param datetime action_timestamp: Time when action took place.
    :param float throughput_mb_s: Rate at which copy took place.

    :returns: A new environment metrics instance.
    :rtype: types.EnvironmentMetric

    """
    instance = types.EnvironmentMetric()
    instance.action_name = unicode(action_name)
    instance.action_timestamp = action_timestamp
    instance.dir_from = unicode(dir_from)
    instance.dir_to = unicode(dir_to)
    instance.duration_ms = duration_ms
    instance.job_uid = unicode(job_uid)
    instance.simulation_uid = unicode(simulation_uid)
    instance.size_mb = size_mb
    instance.throughput_mb_s = throughput_mb_s

    return session.insert(instance)


def get_accounting_projects():
    """Retrieves disinct set of accounting projects.

    """
    j = types.Job

    qry = session.raw_query(
        distinct(j.accounting_project)
        )

    return set(sorted([ap[0] for ap in qry.all()]))
