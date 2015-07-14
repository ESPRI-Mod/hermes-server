# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.monitoring.fetch_one.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import base64

from prodiguer.db import pgres as db
from prodiguer.web.request_validation import validator_monitoring as rv
from prodiguer.web.utils.http import ProdiguerHTTPRequestHandler



# Query parameter names.
_PARAM_UID = 'uid'



class FetchOneRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation monitor front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _decode_request():
            """Decodes request.

            """
            self.uid = self.get_argument(_PARAM_UID)


        def _get_data(factory):
            """Returns data for front-end.

            """
            data = factory(self.uid)
            try:
                iter(data)
            except TypeError:
                return db.utils.get_item(data)
            else:
                return db.utils.get_collection(data)


        def _get_configuration_card(uid):
            """Returns simulation configuration card.

            """
            configuration = db.dao_monitoring.retrieve_simulation_configuration(uid)

            return base64.b64decode(configuration.card) if configuration else ''


        def _set_output():
            """Sets response to be returned to client.

            """
            db.session.start()
            self.output = {
                'job_history':
                    _get_data(db.dao_monitoring.retrieve_simulation_jobs),
                'simulation':
                    _get_data(db.dao_monitoring.retrieve_simulation),
                'config_card':
                    _get_configuration_card(self.uid)
            }
            db.session.end()

        # Invoke tasks.
        self.invoke(rv.validate_fetch_one, [
            _decode_request,
            _set_output
        ])
