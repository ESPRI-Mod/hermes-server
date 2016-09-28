# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.monitoring.fetch_messages.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Messaging fetch messages request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring
from prodiguer.db.pgres import dao_mq
from prodiguer.utils import logger
from prodiguer.web.utils.http1 import process_request



# Query parameter names.
_PARAM_UID = 'uid'


class FetchMessagesRequestHandler(tornado.web.RequestHandler):
    """Simulation monitor fetch messages request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _set_criteria():
            """Sets search criteria.

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


        def _cleanup():
            """Performs cleanup after request processing.

            """
            del self.simulation_uid
            del self.simulation
            del self.message_history


        # Process request.
        process_request(self, [
            _set_criteria,
            _set_data,
            _set_output,
            _cleanup
            ])
