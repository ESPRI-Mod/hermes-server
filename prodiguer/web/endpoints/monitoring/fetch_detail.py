# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.monitoring.fetch_detail.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation detail front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from hermes.db import pgres as db
from hermes.db.pgres.dao_monitoring import retrieve_simulation
from hermes.db.pgres.dao_monitoring import retrieve_simulation_jobs
from hermes.db.pgres.dao_monitoring import retrieve_simulation_configuration
from hermes.db.pgres.dao_monitoring import retrieve_latest_job_period
from hermes.db.pgres.dao_monitoring import retrieve_simulation_previous_tries
from hermes.db.pgres.dao_mq import has_messages
from hermes.utils import logger
from hermes.web.utils.http1 import process_request



# Query parameter names.
_PARAM_UID = 'uid'


class FetchDetailRequestHandler(tornado.web.RequestHandler):
    """Simulation monitor front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _set_criteria():
            """Sets search criteria.

            """
            self.uid = self.get_argument(_PARAM_UID)


        def _set_data():
            """Pulls data from db.

            """
            with db.session.create():
                logger.log_web("[{}]: executing db query: retrieve_simulation_try".format(id(self)))
                self.simulation = retrieve_simulation(self.uid)

                logger.log_web("[{}]: executing db query: retrieve_simulation_jobs".format(id(self)))
                self.job_list = retrieve_simulation_jobs(self.uid)

                logger.log_web("[{}]: executing db query: retrieve_simulation_configuration".format(id(self)))
                self.configuration = retrieve_simulation_configuration(self.uid)

                logger.log_web("[{}]: executing db query: has_messages".format(id(self)))
                self.has_messages = has_messages(self.uid)

                logger.log_web("[{}]: executing db query: retrieve_latest_job_period".format(id(self)))
                self.latest_job_period = retrieve_latest_job_period(self.uid)

                if self.simulation.try_id == 1:
                    self.previous_tries = []
                else:
                    logger.log_web("[{}]: executing db query: retrieve_previous_tries".format(id(self)))
                    self.previous_tries = retrieve_simulation_previous_tries(self.simulation.hashid, self.simulation.try_id)


        def _set_output(self):
            """Sets response to be returned to client.

            """
            self.output = {
                'config_card': self.configuration.card if self.configuration else None,
                'has_messages': self.has_messages,
                'job_list': self.job_list,
                'latest_job_period': self.latest_job_period,
                'previous_tries': self.previous_tries,
                'simulation': self.simulation
            }


        def _cleanup():
            """Performs cleanup after request processing.

            """
            del self.configuration
            del self.has_messages
            del self.job_list
            del self.previous_tries
            del self.simulation
            del self.uid


        # Process request.
        process_request(self, [
            _set_criteria,
            _set_data,
            _set_output,
            _cleanup
            ])
