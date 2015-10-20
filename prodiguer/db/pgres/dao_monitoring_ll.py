# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring_ll.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring data access operations using low-level db driver.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os

import psycopg2

from prodiguer.db import pgres as db
from prodiguer.db.pgres import validator_dao_monitoring as validator
from prodiguer.utils import decorators



# Sql statement for selecting active simulations.
_SQL_SELECT_ACTIVE_SIMULATIONS = """SELECT
    s.accounting_project,
    s.activity,
    s.activity_raw,
    s.compute_node,
    s.compute_node_raw,
    s.compute_node_login,
    s.compute_node_login_raw,
    s.compute_node_machine,
    s.compute_node_machine_raw,
    to_char(s.execution_end_date, 'YYYY-MM-DD HH24:MI:ss.US'),
    to_char(s.execution_start_date, 'YYYY-MM-DD HH24:MI:ss.US'),
    s.experiment,
    s.experiment_raw,
    s.is_error,
    s.hashid,
    s.model,
    s.model_raw,
    s.name,
    to_char(s.output_end_date, 'YYYY-MM-DD'),
    to_char(s.output_start_date, 'YYYY-MM-DD'),
    s.space,
    s.space_raw,
    s.try_id,
    s.uid
FROM
    monitoring.tbl_simulation as s
WHERE
    s.execution_start_date IS NOT NULL AND
    s.is_obsolete = false
"""

# Sql statement for selecting active jobs.
_SQL_SELECT_ACTIVE_JOBS = """SELECT
    to_char(j.execution_end_date, 'YYYY-MM-DD HH24:MI:ss.US'),
    to_char(j.execution_start_date, 'YYYY-MM-DD HH24:MI:ss.US'),
    j.is_error,
    j.job_uid,
    j.simulation_uid,
    j.typeof
FROM
    monitoring.tbl_job as j
JOIN
    monitoring.tbl_simulation as s ON j.simulation_uid = s.uid
WHERE
    j.execution_start_date IS NOT NULL AND
    s.execution_start_date IS NOT NULL AND
    s.is_obsolete = false
"""

# Sql statement for selecting active timesliaces.
_SQL_SELECT_ACTIVE_TIMESLICE_CRITERIA = " AND s.execution_start_date >= '{}'"

# Sql statement for selecting simulation messages.
_SQL_SELECT_SIMULATION_MESSAGES = """SELECT
    m.content,
    m.email_id,
    m.correlation_id_2,
    to_char(m.row_create_date + INTERVAL '2 hours', 'YYYY-MM-DD HH24:MI:ss.US'),
    m.producer_version,
    to_char(m.timestamp, 'YYYY-MM-DD HH24:MI:ss.US'),
    m.type_id,
    m.uid
FROM
    mq.tbl_message as m
WHERE
    m.correlation_id_1 = '{}' AND
    m.type_id NOT IN ('7000')
ORDER BY
    m.timestamp;
"""

# Sql statement for selecting simulation jobs.
_SQL_SELECT_SIMULATION_JOBS = """SELECT
    j.accounting_project,
    to_char(j.execution_end_date, 'YYYY-MM-DD HH24:MI:ss.US'),
    to_char(j.execution_start_date, 'YYYY-MM-DD HH24:MI:ss.US'),
    j.is_error,
    j.is_compute_end,
    j.job_uid,
    j.post_processing_component,
    to_char(j.post_processing_date, 'YYYY-MM-DD'),
    j.post_processing_dimension,
    j.post_processing_file,
    j.post_processing_name,
    j.simulation_uid,
    j.scheduler_id,
    j.submission_path,
    j.typeof,
    j.warning_delay
FROM
    monitoring.tbl_job as j
WHERE
    j.execution_start_date IS NOT NULL AND
    j.simulation_uid = '{}'
ORDER BY
    j.execution_start_date;
"""



def _get_psycopg2_connection():
    """Returns a pscopg2 connection to the db.

    """
    return psycopg2.connect(
        database=db.constants.PRODIGUER_DB_NAME,
        user=db.constants.PRODIGUER_DB_USER,
        host=os.getenv("PRODIGUER_DB_PGRES_HOST").split(":")[0],
        password=os.getenv("PRODIGUER_DB_PGRES_USER_PASSWORD")
        )


def _fetch_all(sql):
    """Executes a sql statement and return data returned by cursor.

    """
    conn = _get_psycopg2_connection()
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    conn.close()

    return data


@decorators.validate(validator.validate_retrieve_active_simulations)
def retrieve_active_simulations(start_date=None):
    """Retrieves active simulation details from db.

    :param datetime.datetime start_date: Simulation execution start date.

    :returns: Simulation details.
    :rtype: list

    """
    sql = _SQL_SELECT_ACTIVE_SIMULATIONS
    if start_date:
        sql += _SQL_SELECT_ACTIVE_TIMESLICE_CRITERIA.format(start_date)
    sql += ";"

    return _fetch_all(sql)


@decorators.validate(validator.validate_retrieve_active_jobs)
def retrieve_active_jobs(start_date=None):
    """Retrieves active job details from db.

    :param datetime.datetime start_date: Job execution start date.

    :returns: Job details.
    :rtype: list

    """
    sql = _SQL_SELECT_ACTIVE_JOBS
    if start_date:
        sql += _SQL_SELECT_ACTIVE_TIMESLICE_CRITERIA.format(start_date)
    sql += ";"

    return _fetch_all(sql)


@decorators.validate(validator.validate_retrieve_simulation_messages)
def retrieve_simulation_messages(uid):
    """Retrieves message details from db.

    :param str uid: UID of simulation.

    :returns: List of message associated with a simulation.
    :rtype: list

    """
    sql = _SQL_SELECT_SIMULATION_MESSAGES.format(uid)

    return _fetch_all(sql)


@decorators.validate(validator.validate_retrieve_simulation_jobs)
def retrieve_simulation_jobs(uid):
    """Retrieves job details from db.

    :param str uid: UID of simulation.

    :returns: List of jobs associated with a simulation.
    :rtype: list

    """
    sql = _SQL_SELECT_SIMULATION_JOBS.format(uid)

    return _fetch_all(sql)
