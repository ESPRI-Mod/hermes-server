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

from prodiguer.db import pgres as db
from prodiguer.web.request_validation import validator_monitoring as rv
from prodiguer.web.utils.http import ProdiguerHTTPRequestHandler



# Query parameter names.
_PARAM_TIMESLICE = 'timeslice'


class FetchTimeSliceRequestHandler(ProdiguerHTTPRequestHandler):
    """Fetches a time slice of simulations.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _get_data(factory):
            """Returns data for front-end.

            """
            start_date = self.start_date.datetime if self.start_date else None

            return db.utils.get_collection(factory(start_date))


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
            elif timeslice == 'ALL':
                self.start_date = None


        def _set_output():
            """Sets response to be returned to client.

            """
            db.session.start()
            self.output = {
                'job_history':
                    _get_data(db.dao_monitoring.retrieve_active_jobs),
                'simulation_list':
                    _get_data(db.dao_monitoring.retrieve_active_simulations)
            }
            db.session.end()

        # Invoke tasks.
        self.invoke(rv.validate_fetch_timeslice, [
            _decode_request,
            _set_output,
            ])
