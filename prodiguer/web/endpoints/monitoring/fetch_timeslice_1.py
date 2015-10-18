# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.monitoring.fetch_all.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

import arrow

from prodiguer.db.pgres import dao_monitoring_ll as dao
from prodiguer.web.request_validation import validator_monitoring as rv
from prodiguer.web.utils.http import ProdiguerHTTPRequestHandler



# Query parameter names.
_PARAM_TIMESLICE = 'timeslice'


def trim_job(i):
    """Returns job tuple stripped of superfluous information.

    """
    if i[5] == "post-processing":
        return i[0:5]
    return i


class FetchTimeSlice1RequestHandler(ProdiguerHTTPRequestHandler):
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


        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'jobList': [trim_job(i) for i in dao.retrieve_active_jobs(self.start_date)],
                'simulationList': dao.retrieve_active_simulations(self.start_date)
            }


        # Invoke tasks.
        self.invoke(rv.validate_fetch_timeslice, [
            _decode_request,
            _set_output,
            ], convert_before_write=False)
