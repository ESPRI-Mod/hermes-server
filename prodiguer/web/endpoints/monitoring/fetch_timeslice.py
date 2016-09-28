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
from prodiguer.web.utils import constants
from prodiguer.web.utils.http1 import process_request



# Query parameter names.
_PARAM_TIMESLICE = 'timeslice'


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


        def _set_data():
            """Pulls data from db.

            """
            with db.session.create():
                logger.log_web("[{}]: executing db query: retrieve_active_jobs".format(id(self)))
                self.jobs = dao.retrieve_active_jobs(self.start_date)

                logger.log_web("[{}]: executing db query: retrieve_active_simulations".format(id(self)))
                self.simulations = dao.retrieve_active_simulations(self.start_date)


        def _set_output():
            """Sets response to be returned to client.

            """
            self.write_raw_output = True
            self.output = {
                'jobList': self.jobs,
                'simulationList': self.simulations
            }


        def _cleanup():
            """Performs cleanup after request processing.

            """
            del self.jobs
            del self.simulations
            del self.start_date


        # Process request.
        process_request(self, [
            _set_criteria,
            _set_data,
            _set_output,
            _cleanup
            ])
