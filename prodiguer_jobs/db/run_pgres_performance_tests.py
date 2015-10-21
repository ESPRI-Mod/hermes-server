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

from prodiguer.db import pgres as db
from prodiguer.utils import config
from prodiguer.utils import logger



# Sql statement for selecting simulations.
_SQL_SELECT_SIMULATIONS = """SELECT
    s.*
FROM
    monitoring.tbl_simulation as s
WHERE
    s.execution_start_date IS NOT NULL AND
    s.is_obsolete = false {};
"""

# Sql statement for selecting jobs.
_SQL_SELECT_JOBS = """SELECT
    j.*
FROM
    monitoring.tbl_job as j
JOIN
    monitoring.tbl_simulation as s ON j.simulation_uid = s.uid
WHERE
    s.execution_start_date IS NOT NULL AND
    s.is_obsolete = false {};
"""

# Global now.
_NOW = arrow.utcnow()

# Set of timeslices to test.
_TIMESLICES = [
    "1W",
    "2W",
    "1M",
    "2M",
    "3M",
    "6M",
    "12M",
    "ALL",
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
    "sqlalchemy",
    "psycopg2"
]

# Set of db query targets.
_TARGETS = [
    "simulations",
    "jobs"
]

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
    db.session.start()
    data = timeslice_factory(timeslice_delta)
    db.session.end()

    return data


def _get_psycopg2_connection():
    """Returns a pscopg2 connection to the db.

    """
    return psycopg2.connect(
        database=db.constants.PRODIGUER_DB_NAME,
        user=db.constants.PRODIGUER_DB_USER,
        host=os.getenv("PRODIGUER_DB_PGRES_HOST").split(":")[0],
        password=os.getenv("PRODIGUER_DB_PGRES_USER_PASSWORD")
        )


def exec_psycopg2(timeslice_delta, target):
    """Performs a psycopg2 based db query."""
    if timeslice_delta:
        timeslice_delta = "AND \n\ts.execution_start_date >= '{}'".format(timeslice_delta)
    else:
        timeslice_delta = ""
    conn = _get_psycopg2_connection()
    cur = conn.cursor()
    sql = _PSYCOPG2_FACTORIES[target].format(timeslice_delta)
    cur.execute(sql)
    data = cur.fetchall()
    conn.close()

    return data


# Map of db drivers to functions that perform queries.
_DATA_FACTORIES = {
    "sqlalchemy": exec_sqlalchmey,
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
        os.getenv("PRODIGUER_MACHINE_TYPE"), _NOW.format('YYYY-MM-DD'))
    fpath = os.path.join(os.getenv("PRODIGUER_HOME"), "tmp")
    fpath = os.path.join(fpath, fname)
    with open(fpath, 'wb') as output_file:
        writer = csv.writer(output_file)
        writer.writerow(_CSV_HEADERS)
        writer.writerows(_get_metrics())
    logger.log_db("metrics written to --> {}".format(fpath))


if __name__ == '__main__':
    _main()
