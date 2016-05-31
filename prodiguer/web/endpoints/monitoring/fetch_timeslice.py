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

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer.utils import logger
from prodiguer.web.request_validation import validator_monitoring as rv
from prodiguer.web.utils.http import HermesHTTPRequestHandler



# Query parameter names.
_PARAM_TIMESLICE = 'timeslice'


class FetchTimeSliceRequestHandler(HermesHTTPRequestHandler):
    """Fetches a time slice of simulations.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _decode_request():
            """Decodes request.

            """
            timeslice = self.get_argument(_PARAM_TIMESLICE)
            if timeslice == '1W':
                self.start_date = arrow.now() - datetime.timedelta(days=7)
            elif timeslice == '2W':
                self.start_date = arrow.now() - datetime.timedelta(days=14)
            elif timeslice == '1M':
                self.start_date = arrow.now() - datetime.timedelta(days=31)
            elif timeslice == '2M':
                self.start_date = arrow.now() - datetime.timedelta(days=61)
            elif timeslice == '3M':
                self.start_date = arrow.now() - datetime.timedelta(days=92)
            elif timeslice == '6M':
                self.start_date = arrow.now() - datetime.timedelta(days=183)
            elif timeslice == '12M':
                self.start_date = arrow.now() - datetime.timedelta(days=365)
            elif timeslice == '*':
                self.start_date = None
            if self.start_date is not None:
                self.start_date = self.start_date.datetime


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
            self.output = {
                'jobList': self.jobs,
                'simulationList': self.simulations
            }


        # Invoke tasks.
        self.invoke(rv.validate_fetch_timeslice, [
            _decode_request,
            _set_data,
            _set_output,
            ], write_raw_output=True)
