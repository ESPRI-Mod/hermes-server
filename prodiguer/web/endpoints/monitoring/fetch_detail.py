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
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer.utils import logger
from prodiguer.web.request_validation import validator_monitoring as rv
from prodiguer.web.utils.http import ProdiguerHTTPRequestHandler



# Query parameter names.
_PARAM_HASHID = 'hashid'
_PARAM_TRYID = 'tryID'



class FetchDetailRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation monitor front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _decode_request():
            """Decodes request.

            """
            self.hashid = self.get_argument(_PARAM_HASHID)
            self.try_id = self.get_argument(_PARAM_TRYID)


        def _set_data():
            """Pulls data from db.

            """
            with db.session.create():
                logger.log_web("[{}]: executing db query 1: simulation info".format(id(self)))
                self.simulation = dao.retrieve_simulation_try(self.hashid, self.try_id)

                logger.log_web("[{}]: executing db query 2: simulation jobs".format(id(self)))
                self.job_list = dao.retrieve_simulation_jobs(self.simulation.uid)

                logger.log_web("[{}]: executing db query 3: simulation configuration".format(id(self)))
                self.configuration = dao.retrieve_simulation_configuration(self.simulation.uid)

                logger.log_web("[{}]: executing db query 4: simulation message count".format(id(self)))
                self.message_count = dao.retrieve_simulation_message_count(self.simulation.uid)

                logger.log_web("[{}]: retrieved simulation data".format(id(self)))


        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'config_card': self.configuration.card if self.configuration else None,
                'job_list': self.job_list,
                'message_count': self.message_count,
                'simulation': self.simulation
            }

        # Invoke tasks.
        self.invoke(rv.validate_fetch_one, [
            _decode_request,
            _set_data,
            _set_output
        ])
