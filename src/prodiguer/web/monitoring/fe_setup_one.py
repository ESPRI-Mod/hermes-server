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


def _get_configuration(uid):
    """Returns configuration card.

    """
    configuration = db.dao_monitoring.retrieve_simulation_configuration(uid)

    return base64.b64decode(configuration.card) if configuration else ''


class FrontEndSetupOneRequestHandler(tornado.web.RequestHandler):
    """Simulation monitor front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        # Unpack query parameters.
        uid = self.get_argument(_PARAM_UID)

        # Start db session.
        db.session.start()

        # Load setup data from db.
        data = {
            'job_history':
                _get_data(db.dao_monitoring.retrieve_simulation_jobs(uid)),
            'simulation':
                _get_data(db.dao_monitoring.retrieve_simulation(uid)),
            'config_card':
                _get_configuration(uid)
            }

        # End db session.
        db.session.end()

        # Write response.
        utils_handler.write_json_response(self, data)
