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


def get_item(entity):
    """Returns a db entity formatted for front-end.

    :param db.types.Entity entity: Entity instance.

    :returns: Entity information in dictionary format ready to be returned to front-end.
    :rtype: dict

    """
    # Convert to a dictionary.
    d = db.types.Convertor.to_dict(entity)

    # Remove row meta-info.
    del d["row_create_date"]
    del d["row_update_date"]

    # Format date fields
    for k, v in d.items():
        if type(v) == datetime.datetime:
            d[k] = str(v)[:10]

    return d


def get_simulation_dict(s):
    """Returns simulation information formatted for front-end.

    :param db.types.Simulation s: Simulation instance.

    :returns: Simulation information in dictionary format ready to be returned to front-end.
    :rtype: dict

    """
    # Convert to dict.
    d = get_item(s)

    # Set names
    d["activity"] = _get_name(db.types.Activity, s.activity_id)
    d["compute_node"] = _get_name(db.types.ComputeNode, s.compute_node_id)
    d["compute_node_login"] = _get_name(db.types.ComputeNodeLogin, s.compute_node_login_id),
    d["compute_node_machine"] = _get_name(db.types.ComputeNodeMachine, s.compute_node_machine_id)
    d["execution_state"] = _get_name(db.types.SimulationState, s.execution_state_id)
    d["experiment"] = _get_name(db.types.Experiment, s.experiment_id)
    d["model"] = _get_name(db.types.Model, s.model_id)
    d["space"] = _get_name(db.types.SimulationSpace, s.space_id)

    return d