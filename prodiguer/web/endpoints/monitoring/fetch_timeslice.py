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
            self.sort_field = self.get_argument(_PARAM_SORT_FIELD)
            self.sort_direction = self.get_argument(_PARAM_SORT_DIRECTION)


        def _set_data():
            """Pulls data from db.

            """
            with db.session.create():
                logger.log_web("[{}]: executing db query: retrieve_active_simulations".format(id(self)))
                self.simulations = \
                    dao.retrieve_active_simulations(self.start_date)

                logger.log_web("[{}]: executing db queries: retrieve_active_jobset, retrieve_active_jobperiodset".format(id(self)))
                self.jobs, self.job_periods = \
                    _get_job_timeslice(self.simulations, self.sort_field, self.sort_direction)


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


def _get_job_timeslice(
    simulations,
    sort_field,
    sort_direction,
    offset=300
    ):
    """Returns job timeslice - a subset of full job timeslice.

    """
    # Format incoming sort field.
    if sort_field == "accountingProject":
        sort_field = "accounting_project"
    if sort_field == "computeNodeLogin":
        sort_field = "compute_node_login"
    if sort_field == "computeNodeMachine":
        sort_field = "compute_node_machine"

    # Apply sort.
    simulations = sorted(simulations, key=lambda i: getattr(i, sort_field))
    if sort_direction == "desc":
        simulations.reverse()

    # Apply filter.
    # TODO

    # Apply offset.
    simulation_identifers = [i.id for i in simulations[:offset]]

    # Return job data.
    return dao.retrieve_active_jobs(None, simulation_identifers), \
           dao.retrieve_active_job_periods(None, simulation_identifers)
