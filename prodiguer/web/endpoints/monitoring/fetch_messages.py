# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.monitoring.fetch_messages.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Messaging fetch messages request handler.

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


class FetchMessagesRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation monitor fetch messages request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _decode_request():
            """Decodes request.

            """
            self.simulation_uid = self.get_argument(_PARAM_UID)


        def _set_data():
            """Pulls data from db.

            """
            with db.session.create():
                logger.log_web("[{}]: executing db query: retrieve_simulation".format(id(self)))
                self.simulation = dao_monitoring.retrieve_simulation(self.simulation_uid)

                logger.log_web("[{}]: executing db query: retrieve_messages".format(id(self)))
                self.message_history = dao_mq.retrieve_messages(self.simulation_uid)


        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'message_history': self.message_history,
                'simulation': self.simulation
            }


        # Invoke tasks.
        self.invoke(rv.validate_fetch_messages, [
            _decode_request,
            _set_data,
            _set_output
        ])
