# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao.py
   :platform: Unix
   :synopsis: Set of core data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import random

from . import constants, session, types
from .types import (
    Activity,
    ComputeNode,
    ComputeNodeLogin,
    ComputeNodeMachine,
    Experiment,
    ExperimentGroup,
    Institute,
    Message,
    Model,
    Simulation,
    SimulationForcing,
    SimulationStateChange
    )
from ..utils import runtime as rt



def sort(etype, collection):
    """Sorts collection via type sort key.

    :param etype: A supported entity type.
    :type etype: class

    :param collection: Collection of entities.
    :type collection: list

    :returns: Sorted collection.
    :rtype: list

    """
    types.assert_type(etype)

    return [] if collection is None else etype.get_sorted(collection)


def get_active(etype):
    """Gets all active instances.

    :param etype: A supported entity type.
    :type etype: class

    :returns: Active entity collection.
    :rtype: list

    """
    return get_by_facet(etype, qfilter=etype.is_active==True, get_iterable=True)


def get_all(etype):
    """Gets all instances of the entity.

    :param etype: A supported entity type.
    :type etype: class

    :returns: Entity collection.
    :rtype: list

    """
    return get_by_facet(etype, order_by=etype.id, get_iterable=True)


def get_by_facet(etype, qfilter=None, order_by=None, get_iterable=False):
    """Gets entity instance by facet.

    :param class etype: A supported entity type.
    :param expression qfilter: Query filter expression.
    :param expression order_by: Sort expression.
    :param bool get_iterable: Flag indicating whether to return an iterable or not.

    :returns: Entity or entity collection.
    :rtype: Sub-class of types.Entity

    """
    types.assert_type(etype)

    q = session.query(etype)
    if qfilter is not None:
        q = q.filter(qfilter)
    if order_by is not None:
        q = q.order_by(order_by)

    return sort(etype, q.all()) if get_iterable else q.first()


def get_random(etype):
    """Returns a random instance.

    :param etype: Type of instance to be returned.
    :type etype: class

    :returns: A random item from the cache.
    :rtype: Sub-class of types.Entity

    """
    all = get_all(etype)

    return None if not len(all) else all[random.randint(0, len(all) - 1)]


def get_random_sample(etype):
    """Returns a random instance sample.

    :param etype: Type of instances to be returned.
    :type etype: class

    :returns: A random sample from the db.
    :rtype: list

    """
    all = get_all(etype)

    return [] if not len(all) else random.sample(all, random.randint(1, len(all)))


def get_by_id(etype, id):
    """Gets entity instance by id.

    :param etype: A supported entity type.
    :type etype: class

    :param id: id of entity.
    :type id: int

    :returns: Entity with matching id.
    :rtype: Sub-class of types.Entity

    """
    return get_by_facet(etype, qfilter=etype.id==id)


def get_by_name(etype, name):
    """Gets an entity instance by it's name.

    :param etype: A supported entity type.
    :type etype: class

    :param name: Name of entity.
    :type name: str

    :returns: Entity with matching name.
    :rtype: Sub-class of types.Entity

    """
    return get_by_facet(etype, qfilter=etype.name==name)


def get_count(etype, qfilter=None):
    """Gets count of entity instances.

    :param etype: A supported entity type.
    :type etype: class

    :returns: Entity collection count.
    :rtype: int

    """
    types.assert_type(etype)

    q = session.query(etype)
    if qfilter is not None:
        q = q.filter(qfilter)

    return q.count()


def insert(entity):
    """Adds a newly created model to the session.

    :param item: A supported entity instance.
    :type item: Sub-class of types.Entity

    """
    session.add(entity)

    return entity


def delete(entity):
    """Marks entity instance for deletion.

    :param item: A supported entity instance.
    :type item: Sub-class of types.Entity

    """
    session.delete(entity)


def delete_all(etype):
    """Deletes all entities of passed type.

    :param etype: A supported entity type.
    :type etype: class

    """
    types.assert_type(etype)

    q = session.query(etype)
    q.delete()


def delete_by_facet(etype, expression):
    """Delete entity instance by id.

    :param etype: A supported entity type.
    :type etype: class

    :param facet: Entity facet.
    :type facet: expression

    :param facet: Entity facet value.
    :type facet: object

    """
    types.assert_type(etype)

    q = session.query(etype)
    q = q.filter(expression)
    q.delete()


def delete_by_id(etype, id):
    """Delete entity instance by id.

    :param etype: A supported entity type.
    :type etype: class

    :param id: id of entity.
    :type id: int

    """
    delete_by_facet(etype, etype.id==id)


def delete_by_name(etype, name):
    """Deletes an entity instance by it's name.

    :param etype: A supported entity type.
    :type etype: class

    :param name: Name of entity.
    :type name: str

    """
    delete_by_facet(etype, etype.name==name)


def get_compute_node_by_institute_and_name(institute, name):
    """Gets a compute node by institute & name.

    """
    q = session.query(ComputeNode, Institute)
    q = q.filter(ComputeNode.name==name)
    q = q.filter(Institute.name==institute)

    return q.first()


def get_compute_node_login_by_login(login):
    """Gets a compute node login by login.

    """
    q = session.query(ComputeNodeLogin)
    q = q.filter(ComputeNodeLogin.login==login)

    return q.first()


def get_compute_node_login_by_is_active():
    """Gets a list of active compute node logins.

    """
    q = session.query(ComputeNodeLogin, ComputeNode)
    q = q.filter(ComputeNodeLogin.is_active==True)
    q = q.filter(ComputeNode.is_active==True)

    return sort(ComputeNodeLogin, q.all())


def get_compute_node_machine_by_is_active():
    """Gets a list of active compute node machines.

    """
    q = session.query(ComputeNodeMachine, ComputeNode)
    q = q.filter(ComputeNodeMachine.is_active==True)
    q = q.filter(ComputeNode.is_active==True)

    return sort(ComputeNodeMachine, q.all())


def get_experiment_by_activity(activity):
    """Gets a list of experiments by activity name.

    """
    q = session.query(Experiment, Activity)
    q = q.filter(Activity.name==activity.upper())

    return sort(Experiment, q.all())


def get_experiment_by_activity_and_name(activity, experiment_name):
    """Gets an experiment by activity and experiment name.

    """
    q = session.query(Experiment, Activity)
    q = q.filter(Activity.name==activity.upper())
    q = q.filter(Experiment.name==experiment_name)

    return q.first()


def get_experiment_by_is_active(activity=None):
    """Gets all active instances.

    """
    q = None
    if activity is not None:
        q = session.query(Experiment, Activity)
        q = q.filter(Activity.name==activity.upper())
    else:
        q = session.query(Experiment)
    q = q.filter(Experiment.is_active==True)

    return sort(Experiment, q.all())


def get_experiment_group_by_activity_and_name(activity, group):
    """Gets an experiment group by activity and experiment name.

    """
    q = session.query(ExperimentGroup, Activity)
    q = q.filter(Activity.name==activity)
    q = q.filter(ExperimentGroup.name==group)

    return q.first()


def get_latest_simulation_state_change(uid):
    """Returns latest simulation state change entry.

    :param str uid: Simulation unique identifier.

    :returns: Most recent simulation state change entry.
    :rtype: SimulationStateChange

    """
    return get_by_facet(
        SimulationStateChange,
        SimulationStateChange.simulation_uid==unicode(uid),
        SimulationStateChange.timestamp.desc(),
        False)


def get_simulations_by_state(state_id):
    """Retrieves list of simulation details from backend filtered by execution state.

    :param state_id: id of execution state.
    :type state_id: int

    :returns: Simulation details.
    :rtype: types.cnode.simulation.Simulation

    """

    if state_id is not None and \
       state_id not in constants.EXECUTION_STATE_SET_ID_LIST:
        rt.throw("Execution state id is invalid : {0}".format(str(state_id)))

    if state_id is None:
        return get_all(Simulation)
    else:
        return get_by_facet(Simulation,
                            Simulation.execution_state_id==state_id,
                            get_iterable=True)


def delete_simulation_forcing_by_simulation_id(simulation_id):
    """Deletes a set of simulation forcings.

    :param simulation_id: id of a simulation.
    :type simulation_id: int

    """
    delete_by_facet(SimulationForcing,
                    SimulationForcing.simulation_id==simulation_id)


