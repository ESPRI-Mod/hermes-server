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

from prodiguer import cv
from prodiguer.db import pgres as db
from prodiguer.utils import config
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


        def _get_simulation_list():
            """Returns simulation data for front-end.

            """
            data = _get_data(db.dao_monitoring.retrieve_active_simulations)

            # Reduce response payload size by deleting unnecessary fields.
            for simulation in data:
                # ... delete fields not required by front end
                del simulation['output_start_date']
                del simulation['output_end_date']
                del simulation['ensemble_member']

                # ... delete null fields
                if simulation['accounting_project'] is None:
                    del simulation['accounting_project']
                if simulation['execution_end_date'] is None:
                    del simulation['execution_end_date']
                if simulation['is_error'] == False:
                    del simulation['is_error']
                if simulation['is_obsolete'] == False:
                    del simulation['is_obsolete']
                if simulation['parent_simulation_branch_date'] is None:
                    del simulation['parent_simulation_branch_date']
                if simulation['parent_simulation_name'] is None:
                    del simulation['parent_simulation_name']

                # ... delete null cv fields
                for field in {
                    'activity',
                    'compute_node',
                    'compute_node_login',
                    'compute_node_machine',
                    'experiment',
                    'model',
                    'space'
                    }:
                    if simulation[field] is None:
                        del simulation[field]
                    if simulation["{}_raw".format(field)] is None:
                        del simulation["{}_raw".format(field)]

            return data


        def _get_job_history():
            """Returns job data for front-end.

            """
            data = _get_data(db.dao_monitoring.retrieve_active_jobs)

            # Reduce response payload size by deleting unnecessary fields.
            for job in data:
                # ... delete null fields
                if job['accounting_project'] is None:
                    del job['accounting_project']
                if job['execution_end_date'] is None:
                    del job['execution_end_date']
                if job['is_error'] == False:
                    del job['is_error']
                if job['post_processing_component'] is None:
                    del job['post_processing_component']
                if job['post_processing_date'] is None:
                    del job['post_processing_date']
                if job['post_processing_dimension'] is None:
                    del job['post_processing_dimension']
                if job['post_processing_file'] is None:
                    del job['post_processing_file']
                if job['post_processing_name'] is None:
                    del job['post_processing_name']

                # ... delete start up field for post-processing jobs
                if job['typeof'] != cv.constants.JOB_TYPE_COMPUTING:
                    del job['is_startup']

                # ... delete fields with matching defaults
                if job['typeof'] == cv.constants.JOB_TYPE_POST_PROCESSING:
                    del job['typeof']
                if job['warning_delay'] == config.apps.monitoring.defaultJobWarningDelayInSeconds:
                    del job['warning_delay']


            return data


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
                'job_history': _get_job_history(),
                'simulation_list': _get_simulation_list()
            }
            db.session.end()


        # Invoke tasks.
        self.invoke(rv.validate_fetch_timeslice, [
            _decode_request,
            _set_output,
            ])
