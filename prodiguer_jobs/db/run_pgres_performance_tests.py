# -*- coding: utf-8 -*-

"""
.. module:: run_pgres_performance_tests.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Executes prodiguer postgres database performance tests and writes results to file system.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import csv
import os

import arrow
import psycopg2
from sqlalchemy import func

from prodiguer.db import pgres as db
from prodiguer.utils import config
from prodiguer.utils import logger



# Sql statement for selecting simulations.
_SQL_SELECT_SIMULATIONS = """SELECT
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
    s.is_obsolete = false {}
ORDER BY
    s.execution_start_date;
"""


# Sql statement for selecting jobs.
_SQL_SELECT_JOBS = """SELECT
    to_char(j.execution_end_date, 'YYYY-MM-DD HH24:MI:ss.US'),
    to_char(j.execution_start_date, 'YYYY-MM-DD HH24:MI:ss.US'),
    j.is_compute_end,
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
    s.is_obsolete = false {}
ORDER BY
    j.execution_start_date;
"""


# Global now.
_NOW = arrow.utcnow()

# Set of timeslices to test.
_TIMESLICES = [
    "1W",
    # "2W",
    # "1M",
    # "2M",
    # "3M",
    # "6M",
    # "12M",
    # "ALL",
]

# Map of timeslice tokens to time deltas.
_TIMESLICE_DELTAS = {
    '1W': _NOW.replace(days=-7),
    '2W': _NOW.replace(days=-14),
    '1M': _NOW.replace(days=-31),
    '2M': _NOW.replace(days=-61),
    '3M': _NOW.replace(days=-92),
    '6M': _NOW.replace(days=-183),
    '12M': _NOW.replace(days=-365),
    'ALL': None,
}

# Set of db drivers to test.
_DRIVERS = [
    # "sqlalchemy",
    "sqlalchemy-direct",
    "psycopg2"
]

# Set of db query targets.
_TARGETS = [
    "simulations",
    "jobs"
]

def retrieve_active_simulations_direct(start_date=None):
    """Retrieves active simulation details from db.

    :param datetime.datetime start_date: Simulation execution start date.

    :returns: Simulation details.
    :rtype: list

    """

    s = db.types.Simulation
    qry = db.session.raw_query(
        s.accounting_project,
        s.activity,
        s.activity_raw,
        s.compute_node,
        s.compute_node_raw,
        s.compute_node_login,
        s.compute_node_login_raw,
        s.compute_node_machine,
        s.compute_node_machine_raw,
        db.as_datetime_string(s.execution_end_date),
        db.as_datetime_string(s.execution_start_date),
        s.experiment,
        s.experiment_raw,
        s.is_error,
        s.hashid,
        s.model,
        s.model_raw,
        s.name,
        db.as_date_string(s.output_end_date),
        db.as_date_string(s.output_start_date),
        s.space,
        s.space_raw,
        s.try_id,
        s.uid
        )
    qry = qry.filter(s.execution_start_date != None)
    qry = qry.filter(s.is_obsolete == False)
    if start_date:
        qry = qry.filter(s.execution_start_date >= start_date)

    sims = qry.all()

    print sims[0]

    return sims

    return qry.all()


def retrieve_active_jobs_direct(start_date=None):
    """Retrieves active job details from db.

    :param datetime.datetime start_date: Job execution start date.

    :returns: Job details.
    :rtype: list

    """
    j = db.types.Job
    s = db.types.Simulation
    qry = db.session.raw_query(
        j.execution_end_date,
        j.execution_start_date,
        j.is_compute_end,
        j.is_error,
        j.job_uid,
        j.simulation_uid,
        j.typeof
        )
    qry = qry.join(s, j.simulation_uid == s.uid)
    qry = qry.filter(j.execution_start_date != None)
    qry = qry.filter(s.execution_start_date != None)
    qry = qry.filter(s.is_obsolete == False)
    if start_date:
        qry = qry.filter(s.execution_start_date >= start_date)

    return qry.all()


def retrieve_active_simulations(start_date=None):
    """Retrieves active simulation details from db.

    :param datetime.datetime start_date: Simulation execution start date.

    :returns: Simulation details.
    :rtype: list

    """
    qry = db.session.query(db.types.Simulation)
    qry = qry.filter(db.types.Simulation.execution_start_date != None)
    qry = qry.filter(db.types.Simulation.is_obsolete == False)
    if start_date is not None:
        qry = qry.filter(db.types.Simulation.execution_start_date >= start_date)

    return qry.all()


def retrieve_active_jobs(start_date=None):
    """Retrieves active job details from db.

    :param datetime.datetime start_date: Job execution start date.

    :returns: Job details.
    :rtype: list

    """
    qry = db.session.query(db.types.Job)
    qry = qry.join(db.types.Simulation, db.types.Job.simulation_uid==db.types.Simulation.uid)
    qry = qry.filter(db.types.Job.execution_start_date != None)
    qry = qry.filter(db.types.Simulation.execution_start_date != None)
    qry = qry.filter(db.types.Simulation.is_obsolete == False)
    if start_date is not None:
        qry = qry.filter(db.types.Simulation.execution_start_date >= start_date)

    return qry.all()


# Map of db query targets to sqlalchmey based functions.
_SQLALCHEMY_FACTORIES = {
    "simulations": retrieve_active_simulations,
    "jobs": retrieve_active_jobs,
}

# Map of db query targets to sqlalchmey based functions.
_SQLALCHEMY_FACTORIES_DIRECT = {
    "simulations": retrieve_active_simulations_direct,
    "jobs": retrieve_active_jobs_direct,
}

# Map of db query targets to psycopg2 sql statements.
_PSYCOPG2_FACTORIES = {
    "simulations": _SQL_SELECT_SIMULATIONS,
    "jobs": _SQL_SELECT_JOBS,
}


def exec_sqlalchmey(timeslice_delta, target):
    """Performs a sqlalchemy based db query."""
    if timeslice_delta:
        timeslice_delta = timeslice_delta.datetime
    timeslice_factory = _SQLALCHEMY_FACTORIES[target]
    with db.session.create():
        data = timeslice_factory(timeslice_delta)

    return data


def exec_sqlalchmey_direct(timeslice_delta, target):
    """Performs a sqlalchemy based db query."""
    if timeslice_delta:
        timeslice_delta = timeslice_delta.datetime
    timeslice_factory = _SQLALCHEMY_FACTORIES_DIRECT[target]
    with db.session.create():
        data = timeslice_factory(timeslice_delta)

    return data


def exec_psycopg2(timeslice_delta, target):
    """Performs a psycopg2 based db query."""
    if timeslice_delta:
        timeslice_delta = "AND \n\ts.execution_start_date >= '{}'".format(timeslice_delta)
    else:
        timeslice_delta = ""
    conn = psycopg2.connect(
        database=db.constants.PRODIGUER_DB_NAME,
        user=db.constants.PRODIGUER_DB_USER,
        host=os.getenv("HERMES_DB_PGRES_HOST").split(":")[0],
        password=os.getenv("HERMES_DB_PGRES_USER_PASSWORD")
        )
    cur = conn.cursor()
    sql = _PSYCOPG2_FACTORIES[target].format(timeslice_delta)
    cur.execute(sql)
    data = cur.fetchall()
    conn.close()

    return data


# Map of db drivers to functions that perform queries.
_DATA_FACTORIES = {
    "sqlalchemy": exec_sqlalchmey,
    "sqlalchemy-direct": exec_sqlalchmey_direct,
    "psycopg2": exec_psycopg2
}

# Set of CSV file headers.
_CSV_HEADERS = ("TARGET", "TIMESLICE", "DB DRIVER", "RETURNED ROW COUNT", "QUERY TIME")


def _get_metric(target, timeslice, driver):
    """Returns a db performance metric.

    """
    timeslice_delta = _TIMESLICE_DELTAS[timeslice]
    data_factory = _DATA_FACTORIES[driver]
    then = arrow.now()
    data = data_factory(timeslice_delta, target)
    time = arrow.now() - then

    return target, timeslice, driver, len(data), time


def _get_metrics():
    """Returns a collection of db performance metrics.

    """
    metrics = []
    for target in _TARGETS:
        for timeslice in _TIMESLICES:
            for driver in _DRIVERS:
                metrics.append(_get_metric(target, timeslice, driver))

    return metrics


def _main():
    """Main entry point.

    """
    fname = "{}_server_pgres_performance_metrics_{}.csv".format(
        os.getenv("HERMES_MACHINE_TYPE"), _NOW.format('YYYY-MM-DD'))
    fpath = os.path.join(os.getenv("HERMES_HOME"), "tmp")
    fpath = os.path.join(fpath, fname)
    with open(fpath, 'wb') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(_CSV_HEADERS)
        writer.writerows(_get_metrics())
    logger.log_db("metrics written to --> {}".format(fpath))


if __name__ == '__main__':
    _main()
