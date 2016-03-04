# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.monitoring.fetch_detail.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation detail front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring
from prodiguer.db.pgres import dao_mq
from prodiguer.utils import logger
from prodiguer.web.request_validation import validator_monitoring as rv
from prodiguer.web.utils.http import ProdiguerHTTPRequestHandler



# Query parameter names.
_PARAM_UID = 'uid'



class FetchDetailRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation monitor front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _decode_request():
            """Decodes request.

            """
            self.uid = self.get_argument(_PARAM_UID)


        def _set_data():
            """Pulls data from db.

            """
            with db.session.create():
                logger.log_web("[{}]: executing db query: retrieve_simulation_try".format(id(self)))
                self.simulation = dao_monitoring.retrieve_simulation(self.uid)

                logger.log_web("[{}]: executing db query: retrieve_simulation_jobs".format(id(self)))
                self.job_list = dao_monitoring.retrieve_simulation_jobs(self.simulation.uid)

                logger.log_web("[{}]: executing db query: retrieve_simulation_configuration".format(id(self)))
                self.configuration = dao_monitoring.retrieve_simulation_configuration(self.simulation.uid)

                logger.log_web("[{}]: executing db query: retrieve_message_count".format(id(self)))
                self.has_messages = dao_mq.has_messages(self.simulation.uid)


        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'config_card': self.configuration.card if self.configuration else None,
                'has_messages': self.has_messages,
                'job_list': self.job_list,
                'simulation': self.simulation
            }


        # Invoke tasks.
        self.invoke(rv.validate_fetch_one, [
            _decode_request,
            _set_data,
            _set_output
        ])
