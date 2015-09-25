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
from prodiguer.db.pgres import dao_monitoring as dao
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
        def _get_message_history():
            """Returns simulation message history for front-end.

            """
            data = db.utils.get_collection(dao.retrieve_simulation_messages(self.simulation_uid))

            # Set job identifier.
            for msg in data:
                msg['job_uid'] = msg['correlation_id_2']

            # Reduce response payload size by deleting unnecessary fields.
            for msg in data:
                del msg['app_id']
                del msg['content_type']
                del msg['content_encoding']
                del msg['correlation_id_1']
                del msg['correlation_id_2']
                del msg['correlation_id_3']
                del msg['producer_id']
                del msg['timestamp_raw']
                del msg['timestamp_precision']
                del msg['user_id']

            return data


        def _decode_request():
            """Decodes request.

            """
            self.simulation_uid = self.get_argument(_PARAM_UID)


        def _set_output():
            """Sets response to be returned to client.

            """
            db.session.start()
            self.output = {
                'message_history': _get_message_history(),
                'simulation': dao.retrieve_simulation(self.simulation_uid)
            }
            db.session.end()

        # Invoke tasks.
        self.invoke(rv.validate_fetch_messages, [
            _decode_request,
            _set_output
        ])
