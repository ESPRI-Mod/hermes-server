# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.sim_metrics._validator.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric API request validators.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import uuid

from voluptuous import All, Required, Schema

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer.web.utils.request_validation import Sequence
from prodiguer.web.utils.request_validation import validate



# Query parameter names.
_PARAM_UID = 'uid'


def _SimulationUID():
    """Validates incoming simulation uid query parameter.

    """
    def f(val):
        """Inner function.

        """
        db.session.start()
        uid = unicode(val[0])
        if not dao.exists(uid):
            raise ValueError("Simulation {0} not found".format(uid))
        db.session.end()

    return f


def validate_event(handler):
    """Validates event endpoint HTTP request.

    """
    # TODO
    pass


def validate_fetch_cv(handler):
    """Validates fetch_cv endpoint HTTP request.

    """
    validate(handler)


def validate_fetch_all(handler):
    """Validates fetch_all endpoint HTTP request.

    """
    validate(handler)


def validate_fetch_one(handler):
    """Validates fetch_one endpoint HTTP request.

    """
    def _query_validator(handler):
        """Validates HTTP request query arguments.

        """
        schema = Schema({
            Required(_PARAM_UID): All(list, Sequence(uuid.UUID), _SimulationUID())
        })
        schema(handler.request.query_arguments)

    validate(handler, query_validator=_query_validator)


def validate_websocket(handler):
    """Validates websocket endpoint HTTP request.

    """
    validate(handler)
