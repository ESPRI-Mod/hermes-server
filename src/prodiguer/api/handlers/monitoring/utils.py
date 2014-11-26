# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.monitoring.utils.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring utility functions.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import datetime

from .... import db



def _get_name(entity_type, entity_id):
    """Utility function to map a db entity id to an entity name.

    :param db.types.Entity entity: Entity instance.

    :returns: Entity instance name.
    :rtype: str

    """
    return db.cache.get_name(entity_type, entity_id)


def get_list(entity_type):
    """Returns a list of db entities formatted for front-end.

    :param db.types.Entity entity: Entity instance.

    :returns: A list of entites in dictionary format ready to be returned to front-end.
    :rtype: list

    """
    return [get_item(e) for e in db.dao.get_all(entity_type)]


def format_date_fields(obj):
    """Formats date fields within passed dictionary.

    """
    for key, val in obj.items():
        if type(val) == datetime.datetime:
            obj[key] = str(val)[:10]


def get_item(entity):
    """Returns a db entity formatted for front-end.

    :param db.types.Entity entity: Entity instance.

    :returns: Entity information in dictionary format ready to be returned to front-end.
    :rtype: dict

    """
    # Convert to a dictionary.
    obj = db.types.Convertor.to_dict(entity)

    # Remove row meta-info.
    del obj["row_create_date"]
    del obj["row_update_date"]

    # Format date fields
    format_date_fields(obj)

    return obj


def get_simulation_dict(s):
    """Returns simulation information formatted for front-end.

    :param db.types.Simulation s: Simulation instance.

    :returns: Simulation information in dictionary format ready to be returned to front-end.
    :rtype: dict

    """
    # Convert to dict.
    obj = get_item(s)

    # Set names
    obj["activity"] = _get_name(db.types.Activity, s.activity_id)
    obj["compute_node"] = _get_name(db.types.ComputeNode, s.compute_node_id)
    obj["compute_node_login"] = _get_name(db.types.ComputeNodeLogin, s.compute_node_login_id),
    obj["compute_node_machine"] = _get_name(db.types.ComputeNodeMachine, s.compute_node_machine_id)
    obj["execution_state"] = _get_name(db.types.SimulationState, s.execution_state_id)
    obj["experiment"] = _get_name(db.types.Experiment, s.experiment_id)
    obj["model"] = _get_name(db.types.Model, s.model_id)
    obj["space"] = _get_name(db.types.SimulationSpace, s.space_id)

    return obj


def get_simulation_state_change_dict(ssc):
    """Returns simulation information formatted for front-end.

    :param db.types.Simulation ssc: Simulation state change instance.

    :returns: Simulation state change information in dictionary format ready to be returned to front-end.
    :rtype: dict

    """
    # Convert to dict.
    d = get_item(ssc)

    # Set names
    d["state"] = _get_name(db.types.SimulationState, ssc.state_id)

    return d
