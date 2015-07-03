# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.sim_metrics._validator.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric API request validators.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import voluptuous



# Query parameter names.
_PARAM_SIMULATION_UID = 'uid'


def _SimulationUID():
    """Validates incoming simulation uid query parameter.

    """
    def f(val):
        """Inner function.

        """
        _validate_group_name(val[0])

    return f


def validate_fetch_cv(handler):
    """Validates fetch_cv endpoint HTTP request.

    """
    print 'validate cv'


def validate_fetch_all(handler):
    """Validates fetch_all endpoint HTTP request.

    """
    print 'validate setup - all'


def validate_fetch_one(handler):
    """Validates fetch_one endpoint HTTP request.

    """
    print 'validate setup - one'


def validate_websocket(handler):
    """Validates websocket endpoint HTTP request.

    """
    pass
