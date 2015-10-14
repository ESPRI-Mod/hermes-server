# -*- coding: utf-8 -*-

"""
.. module:: run_pgres_create_test_db.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Creates a test database that simulates a years worth of simulations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime
import random
import uuid

from prodiguer.db import pgres as db
from prodiguer import cv


# Set of accounting projects to be used.
_ACCOUNTING_PROJECTS = [
    u"ipsl",
    u"lmd",
    u"gencmip6",
    u"gen6328"
]

# The base date.
_BASE_DATE = datetime.datetime.now() + datetime.timedelta(days=1)

# Set of timeslices to test.
_TIMESLICES = [
    "1W",
    "2W",
    "1M",
    "2M",
    "3M",
    "6M",
    "12M",
    "18M"
]

# Map of timeslice tokens to execution start dates.
_EXECUTION_START_DATES = {
    '1W': _BASE_DATE - datetime.timedelta(days=7),
    '2W': _BASE_DATE - datetime.timedelta(days=14),
    '1M': _BASE_DATE - datetime.timedelta(days=28),
    '2M': _BASE_DATE - datetime.timedelta(days=60),
    '3M': _BASE_DATE - datetime.timedelta(days=90),
    '6M': _BASE_DATE - datetime.timedelta(days=180),
    '12M': _BASE_DATE - datetime.timedelta(days=365),
    '18M': _BASE_DATE - datetime.timedelta(days=540)
}

# Set of output end/start data to be used.
_OUTPUT_DATES = [datetime.datetime(1880 + (i * 10), 1, 1) for i in range(15)]


def _get_cv_term(term_type):
    return cv.get_name(cv.cache.get_random_term(term_type))


def _create_job(simulation):
    pass


def _create_simulation(timeslice):
    instance = db.types.Simulation()
    instance.accounting_project = random.choice(_ACCOUNTING_PROJECTS)
    is_error = False
    is_obsolete = False
    instance.execution_start_date = _EXECUTION_START_DATES[timeslice]
    instance.execution_end_date = instance.execution_start_date + datetime.timedelta(days=4)
    instance.name = unicode(uuid.uuid4())[0:15]
    instance.output_start_date = random.choice(_OUTPUT_DATES)
    instance.output_end_date = instance.output_start_date + datetime.timedelta(days=365)
    instance.try_id = 1
    instance.uid = unicode(uuid.uuid4())

    instance.activity = _get_cv_term(cv.constants.TERM_TYPE_ACTIVITY)
    instance.compute_node = _get_cv_term(cv.constants.TERM_TYPE_COMPUTE_NODE)
    instance.compute_node_login = _get_cv_term(cv.constants.TERM_TYPE_COMPUTE_NODE_LOGIN)
    instance.compute_node_machine = _get_cv_term(cv.constants.TERM_TYPE_COMPUTE_NODE_MACHINE)
    instance.experiment = _get_cv_term(cv.constants.TERM_TYPE_EXPERIMENT)
    instance.model = _get_cv_term(cv.constants.TERM_TYPE_MODEL)
    instance.space = _get_cv_term(cv.constants.TERM_TYPE_SIMULATION_SPACE)

    instance.hashid = instance.get_hashid()

    print instance

    return instance


def _main():
    cv.cache.load()
    db.session.start()

    _TIMESLICES.reverse()
    for timeslice in _TIMESLICES:
        db.session.insert(_create_simulation(timeslice))
        break


    db.session.end()



if __name__ == '__main__':
    _main()