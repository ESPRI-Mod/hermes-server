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
        def _get_data(factory):
            """Returns data for front-end.

            """
            start_date = self.start_date.datetime if self.start_date else None

            return factory(start_date)


        def _get_job_list():
            """Returns job data for front-end.

            """
            return [trim_job(i) for i in _get_data(dao.retrieve_active_jobs)]


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


        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'job_list': _get_job_list(),
                'simulation_list': _get_data(dao.retrieve_active_simulations)
            }


        # Invoke tasks.
        self.invoke(rv.validate_fetch_timeslice, [
            _decode_request,
            _set_output,
            ])
