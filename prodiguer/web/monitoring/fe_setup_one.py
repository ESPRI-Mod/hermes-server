# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.monitoring.setup.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import base64
import uuid

import tornado.web
from voluptuous import All, Invalid, Schema, Required

from prodiguer.utils import rt
from prodiguer.web import utils_handler
from prodiguer.db import pgres as db



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_UID = 'uid'



def _validate_get_request_query_arguments(handler):
    """Validates GET endpoint HTTP query arguments.

    """
    def Sequence(expected_type, expected_length=1):
        """Validates a sequence of query parameter values.

        """
        def f(val):
            """Inner function.

            """
            # Validate sequence length.
            if len(val) != expected_length:
                raise ValueError("Invalid request")

            # Validate sequence type.
            for item in val:
                try:
                    expected_type(item)
                except ValueError:
                    raise ValueError("Invalid request")

            return val

        return f


    # Set query argument validation schema.
    schema = Schema({
        Required(_PARAM_UID): All(list, Sequence(uuid.UUID))
    })

    # Apply query argument validation.
    schema(handler.request.query_arguments)


class FrontEndSetupOneRequestHandler(utils_handler.ProdiguerWebServiceRequestHandler):
    """Simulation monitor front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _get_data(data):
            """Returns data for front-end.

            """
            try:
                iter(data)
            except TypeError:
                return db.utils.get_item(data)
            else:
                return db.utils.get_collection(data)


        def _get_simulation_configuration(uid):
            """Returns simulation configuration card.

            """
            configuration = db.dao_monitoring.retrieve_simulation_configuration(uid)

            return base64.b64decode(configuration.card) if configuration else ''


        def _validate_request():
            """Request validator.

            """
            utils_handler.validate_request(self,
                query_validator=_validate_get_request_query_arguments)


        def _decode_request():
            """Decodes request.

            """
            self.uid = self.get_argument(_PARAM_UID)


        def _set_output():
            """Sets response to be returned to client.

            """
            db.session.start()
            self.output = {
                'job_history':
                    _get_data(db.dao_monitoring.retrieve_simulation_jobs(self.uid)),
                'simulation':
                    _get_data(db.dao_monitoring.retrieve_simulation(self.uid)),
                'config_card':
                    _get_simulation_configuration(self.uid)
            }
            db.session.end()

        # Invoke tasks.
        self.invoke(_validate_request, [
            _decode_request,
            _set_output
        ])
