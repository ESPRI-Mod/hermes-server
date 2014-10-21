# -*- coding: utf-8 -*-

"""
.. module:: cv.factory.py
   :copyright: Copyright "May 22, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary factory for creating new cv terms.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
from .. import db



# Default institute.
_DEFAULT_INSITUTE = "IPSL"

# Default experiment group.
_DEFAULT_EXPERIMENT_GROUP = "ipsl-internal"


def _create_term(term_type):
    """Creates a cv term.

    """
    term = term_type()
    term.is_reviewed = False

    return term


def _get_id(term_type, term_name):
    """Utility function to map a name to an id.

    """
    return db.cache.get_id(term_type, term_name)


def create_activity(activity):
    """Creates an activity CV term.

    """
    term = _create_term(db.types.Activity)
    term.description = "{0} ?".format(activity)
    term.name = activity

    return term


def create_compute_node(compute_node, institute=_DEFAULT_INSITUTE):
    """Creates a compute node CV term.

    """
    compute_node = compute_node.upper()

    term = _create_term(db.types.ComputeNode)
    term.description = "{0} ?".format(compute_node)
    term.institute_id = _get_id(db.types.Institute, institute)
    term.name = compute_node

    return term


def create_compute_node_login(compute_node, compute_node_login):
    """Creates a compute node login CV term.

    """
    term = _create_term(db.types.ComputeNodeLogin)
    term.compute_node_id = _get_id(db.types.ComputeNode, compute_node)
    term.login = compute_node_login
    term.first_name = "UNKNOWN"
    term.family_name = "UNKNOWN"
    term.email = "UNKNOWN"

    return term


def create_compute_node_machine(compute_node, compute_node_machine):
    """Creates a compute node machine CV term.

    """
    compute_node = compute_node.upper()
    compute_node_machine = compute_node_machine.upper()

    term = _create_term(db.types.ComputeNodeMachine)
    term.compute_node_id = _get_id(db.types.ComputeNode, compute_node)
    term.manafacturer = "UNKNOWN"
    term.name = "{0} - {1}".format(compute_node, compute_node_machine)
    term.short_name = compute_node_machine
    term.type = "UNKNOWN"

    return term


def create_experiment(activity, experiment, experiment_group=_DEFAULT_EXPERIMENT_GROUP):
    """Creates an experiment CV term.

    """
    term = _create_term(db.types.Experiment)
    term.activity_id = _get_id(db.types.Activity, activity)
    term.description = "{0} ?".format(experiment)
    term.group_id = _get_id(db.types.ExperimentGroup, experiment_group)
    term.name = experiment

    return term


def create_experiment_group(activity, experiment_group):
    """Creates an experiment group CV term.

    """
    term = _create_term(db.types.ExperimentGroup)
    term.activity_id = _get_id(db.types.Activity, activity)
    term.description = "{0} ?".format(experiment_group)
    term.name = experiment_group
    term.ordinal_position = 100000
    term.short_description = "{0} ?".format(experiment_group)
    term.short_description_1 = "{0} ?".format(experiment_group)

    return term


def create_model(model, institute=_DEFAULT_INSITUTE):
    """Creates a model CV term.

    """
    model = model.upper()

    term = _create_term(db.types.Model)
    term.description = "{0} ?".format(model)
    term.drs_tag_name = model
    term.institute_id = _get_id(db.types.Institute, institute)
    term.name = model

    return term


def create_simulation_space(space):
    """Creates a simulation space CV term.

    """
    space = space.upper()

    term = _create_term(db.types.SimulationSpace)
    term.description = "{0} ?".format(space)
    term.name = space

    return term


def create_simulation_state(state):
    """Creates a simulation state CV term.

    """
    state = state.upper()

    term = _create_term(db.types.SimulationState)
    term.description = "{0} ?".format(state)
    term.name = state

    return term
