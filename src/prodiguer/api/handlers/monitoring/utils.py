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
    collection = [get_item(e) for e in db.dao.get_all(entity_type)]

    return collection

def get_sorted_list(entity_type, key='name'):
    """Returns a sorted list of db entities formatted for front-end.

    :param db.types.Entity entity: Entity instance.
    :param expression key: Collection sort key.

    :returns: A sorted list of entites in dictionary format ready to be returned to front-end.
    :rtype: list

    """
    collection = get_list(entity_type)

    return sorted(collection, key=lambda instance: instance[key].lower())


def format_date_fields(obj):
    """Formats date fields within passed dictionary.

    """
    for key, val in obj.items():
        if type(val) == datetime.datetime:
            obj[key] = str(val)[:10]


def get_item(instance):
    """Returns a db entity formatted for front-end.

    :param db.types.Entity entity: Entity instance.

    :returns: Entity information in dictionary format ready to be returned to front-end.
    :rtype: dict

    """
    # Convert to a dictionary.
    obj = db.types.Convertor.to_dict(instance)

    # Set name attribute if required.
    if 'name' not in obj:
        try:
            obj['name'] = instance.name
        except AttributeError:
            pass

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


def get_simulation_filter_facets():
    """Returns simulation filter facets.

    """
    return  {
        'activity_list': get_sorted_list(db.types.Activity),
        'compute_node_list': get_sorted_list(db.types.ComputeNode),
        'compute_node_machine_list': get_sorted_list(db.types.ComputeNodeMachine),
        'compute_node_login_list': get_sorted_list(db.types.ComputeNodeLogin),
        'experiment_list': get_sorted_list(db.types.Experiment),
        'model_list': get_sorted_list(db.types.Model),
        'execution_state_list': get_sorted_list(db.types.SimulationState),
        'space_list': get_sorted_list(db.types.SimulationSpace),
    }
