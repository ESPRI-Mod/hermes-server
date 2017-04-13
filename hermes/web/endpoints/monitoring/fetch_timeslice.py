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

from hermes.db import pgres as db
from hermes.db.pgres.dao_monitoring import retrieve_active_simulations
from hermes.db.pgres.dao_monitoring import retrieve_active_job_counts
from hermes.db.pgres.dao_monitoring import retrieve_latest_active_jobs
from hermes.db.pgres.dao_monitoring import retrieve_latest_active_job_periods
from hermes.utils import logger
from hermes.web.utils.http1 import process_request



# Query parameter names.
_PARAM_TIMESLICE = 'timeslice'

# Map of timeslices to time deltas (in days).
_TIMESLICE_DELTAS = {
    "*": None,
    "1W": 7,
    "2W": 14,
    "1M": 31,
    "2M": 61,
    "3M": 92,
    "6M": 183,
    "12M": 365
}


class FetchTimeSliceRequestHandler(tornado.web.RequestHandler):
    """Fetches a time slice of simulations.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _set_criteria():
            """Sets search criteria.

            """
            delta = _TIMESLICE_DELTAS[self.get_argument(_PARAM_TIMESLICE)]
            self.start_date = None if delta is None else \
                              (arrow.utcnow() - datetime.timedelta(days=delta)).datetime


        def _set_data():
            """Pulls data from db.

            """
            with db.session.create():
                logger.log_web("[{}]: executing db query: retrieve_active_simulations".format(id(self)))
                self.simulations = retrieve_active_simulations(self.start_date)

                logger.log_web("[{}]: executing db query: retrieve_active_job_counts".format(id(self)))
                self.job_counts = retrieve_active_job_counts(self.start_date)

                logger.log_web("[{}]: executing db query: retrieve_latest_active_jobs".format(id(self)))
                self.latest_compute_jobs = retrieve_latest_active_jobs(self.start_date)

                logger.log_web("[{}]: executing db query: retrieve_latest_active_job_periods".format(id(self)))
                self.job_periods = retrieve_latest_active_job_periods(self.start_date)


        def _set_output():
            """Sets response to be returned to client.

            """
            self.write_raw_output = True
            self.output = {
                'jobCounts': self.job_counts,
                'jobPeriodList': self.job_periods,
                'simulationList': self.simulations,
                'latestComputeJobs': self.latest_compute_jobs
            }


        def _cleanup():
            """Performs cleanup after request processing.

            """
            del self.job_counts
            del self.job_periods
            del self.latest_compute_jobs
            del self.simulations
            del self.start_date


        # Process request.
        process_request(self, [
            _set_criteria,
            _set_data,
            _set_output,
            _cleanup
            ])
