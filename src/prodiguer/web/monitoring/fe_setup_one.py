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

import tornado.web

from prodiguer.utils import rt
from prodiguer.web import utils_handler
from prodiguer.db import pgres as db



# Supported content types.
_CONTENT_TYPE_JSON = ["application/json", "application/json; charset=UTF-8"]

# Query parameter names.
_PARAM_UID = 'uid'


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


class FrontEndSetupOneRequestHandler(tornado.web.RequestHandler):
    """Simulation monitor front end setup request handler.

    """
    # @utils_handler.validation_task("GET")
    def _validate_request(self):
        """Validate HTTP GET request.

        """
        # Invalid if body is defined.
        if self.request.body:
            raise ValueError("Invalid request")

        # Invalid if query is defined.
        # if self.request.query:
        #     raise ValueError("Invalid request")
        # print self.request.query_arguments, type(self.request.query_arguments)
        # print self.request.query_arguments['uid'], type(self.request.query_arguments['uid'])
        # print dir(self)
        # print self.get_argument('uid')
        # print dir(self.request)

        # Invalid if files attached.
        if self.request.files:
            raise ValueError("Invalid request")


    # @utils_handler.processing_task("GET", priority=1)
    def _decode_request(self):
        """Decodes request.

        """
        self.uid = self.get_argument(_PARAM_UID)


    # @utils_handler.processing_task("GET", priority=2)
    def _set_output(self):
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


    def get(self, *args):
        """HTTP GET handler.

        """
        validation_tasks = [
            self._validate_request
        ]

        processing_tasks = [
            self._decode_request,
            self._set_output
        ]

        utils_handler.invoke(self, validation_tasks, processing_tasks)
