# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.monitoring.fetch_all.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

import arrow
import tornado

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer.utils import logger
from prodiguer.web.utils.http1 import process_request



# Query parameter names.
_PARAM_TIMESLICE = 'timeslice'
_PARAM_SORT_FIELD = 'sortField'
_PARAM_SORT_DIRECTION = 'sortDirection'
_PARAM_FILTER_ACCOUNTING_PROJECT = 'accountingProject'
_PARAM_FILTER_EXPERIMENT = 'experiment'
_PARAM_FILTER_LOGIN = 'computeNodeLogin'
_PARAM_FILTER_MACHINE = 'computeNodeMachine'
_PARAM_FILTER_MODEL = 'model'
_PARAM_FILTER_SPACE = 'space'



class FetchTimeSliceRequestHandler(tornado.web.RequestHandler):
    """Fetches a time slice of simulations.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _set_criteria():
            """Sets search criteria.

            """
            timeslice = self.get_argument(_PARAM_TIMESLICE)
            if timeslice == '1W':
                start_date = arrow.utcnow() - datetime.timedelta(days=7)
            elif timeslice == '2W':
                start_date = arrow.utcnow() - datetime.timedelta(days=14)
            elif timeslice == '1M':
                start_date = arrow.utcnow() - datetime.timedelta(days=31)
            elif timeslice == '2M':
                start_date = arrow.utcnow() - datetime.timedelta(days=61)
            elif timeslice == '3M':
                start_date = arrow.utcnow() - datetime.timedelta(days=92)
            elif timeslice == '6M':
                start_date = arrow.utcnow() - datetime.timedelta(days=183)
            elif timeslice == '12M':
                start_date = arrow.utcnow() - datetime.timedelta(days=365)

            self.start_date = None if timeslice == '*' else start_date.datetime
            self.criteria = _SearchCriteria(self)


        def _set_data():
            """Pulls data from db.

            """
            with db.session.create():
                logger.log_web("[{}]: executing db query: retrieve_active_simulations".format(id(self)))
                self.simulations = \
                    dao.retrieve_active_simulations(self.start_date)

                logger.log_web("[{}]: executing db queries: retrieve_active_jobset, retrieve_active_jobperiodset".format(id(self)))
                self.jobs, self.job_periods = \
                    _get_job_timeslice(self.simulations, self.criteria)


        def _set_output():
            """Sets response to be returned to client.

            """
            self.write_raw_output = True
            self.output = {
                'jobList': self.jobs,
                'jobPeriodList': self.job_periods,
                'simulationList': self.simulations
            }


        def _cleanup():
            """Performs cleanup after request processing.

            """
            del self.simulations
            del self.start_date


        # Process request.
        process_request(self, [
            _set_criteria,
            _set_data,
            _set_output,
            _cleanup
            ])


def _get_job_timeslice(simulations, criteria, offset=300):
    """Returns job timeslice - a subset of full job timeslice.

    """
    # Format incoming sort field.
    if criteria.sort_field == "accountingProject":
        criteria.sort_field = "accounting_project"
    if criteria.sort_field == "computeNodeLogin":
        criteria.sort_field = "compute_node_login"
    if criteria.sort_field == "computeNodeMachine":
        criteria.sort_field = "compute_node_machine"

    # Apply sort.
    simulations = sorted(simulations, key=lambda i: getattr(i, criteria.sort_field))
    if criteria.sort_direction == "desc":
        simulations.reverse()

    # Apply filters.
    if criteria.accounting_project:
        simulations = [i for i in simulations if i.accounting_project == criteria.accounting_project]
    if criteria.experiment:
        simulations = [i for i in simulations if i.experiment == criteria.experiment]
    if criteria.login:
        simulations = [i for i in simulations if i.compute_node_login == criteria.login]
    if criteria.machine:
        simulations = [i for i in simulations if i.compute_node_machine == criteria.machine]
    if criteria.model:
        simulations = [i for i in simulations if i.model == criteria.model]
    if criteria.space:
        simulations = [i for i in simulations if i.space == criteria.space]
    # TODO state

    # Apply offset.
    simulation_identifers = [i.id for i in simulations[:offset]]

    # Return job data.
    return dao.retrieve_active_jobs(None, simulation_identifers), \
           dao.retrieve_active_job_periods(None, simulation_identifers)


class _SearchCriteria(object):
    """Wraps search criteria dervied from request.

    """
    def __init__(self, handler):
        """Instance constructor.

        """
        self.sort_field = handler.get_argument(_PARAM_SORT_FIELD)
        self.sort_direction = handler.get_argument(_PARAM_SORT_DIRECTION)
        self.accounting_project = handler.get_argument(_PARAM_FILTER_ACCOUNTING_PROJECT, None)
        self.experiment = handler.get_argument(_PARAM_FILTER_EXPERIMENT, None)
        self.login = handler.get_argument(_PARAM_FILTER_LOGIN, None)
        self.machine = handler.get_argument(_PARAM_FILTER_MACHINE, None)
        self.model = handler.get_argument(_PARAM_FILTER_MODEL, None)
        self.space = handler.get_argument(_PARAM_FILTER_SPACE, None)
