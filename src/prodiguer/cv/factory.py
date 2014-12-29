# -*- coding: utf-8 -*-

"""
.. module:: cv.factory.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary factory for creating new cv terms.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import uuid

import arrow

from prodiguer.cv import constants
from prodiguer.utils import rt



# Default institute.
_DEFAULT_INSITUTE = "IPSL"

# Default experiment group.
_DEFAULT_EXPERIMENT_GROUP = "ipsl-internal"


def create1(term_type, term_name, term_data=None):
    """Creates a cv term.

    :param str term_type: Type of CV term being parsed, e.g. activity.
    :param str term_name: Name of CV term being parsed, e.g. ipsl.
    :param dict term_data: User defined term data.

    :returns: Created cv term.
    :rtype: dict

    """
    # Format inputs.
    term_name = unicode(term_name)
    term_type = unicode(term_type.lower())
    term_data = term_data or {}

    # Verify that reserved attribute is not defined.
    if 'meta' in term_data:
        raise ValueError("meta is a reserved CV attribute name")

    # Instantiate term.
    term = {
        "description": term_name,
        "meta": {
            "associations": [],
            "create_date": unicode(arrow.utcnow()),
            "domain": "climate",
            "name": term_name.lower(),
            "status": constants.TERM_GOVERNANCE_STATE_NEW,
            "type": term_type,
            "uid": unicode(uuid.uuid4())
        },
        "name": term_name
    }

    # Update with user defined term data.
    term.update(term_data)

    return term


def create(term_type, term_name):
    """Creates a cv term.

    :param str term_type: Type of CV term being parsed, e.g. activity.
    :param str term_name: Name of CV term being parsed, e.g. ipsl.

    :returns: Created cv term.
    :rtype: db.types.CvTerm

    """
    from prodiguer import db

    term = db.types.CvTerm()
    term.cv_type = term_type
    term.description = "{0} ?".format(term_name)
    term.is_reviewed = False
    term.name = term_name

    rt.log("CV term created: {0}.{1}".format(term.cv_type, term.name))

    return term


def create_activity(activity):
    """Creates an activity CV term.

    """
    return create("activity", activity)


def create_compute_node(compute_node, institute=_DEFAULT_INSITUTE):
    """Creates a compute node CV term.

    """
    compute_node = compute_node.upper()

    return create("compute_node", compute_node)


def create_compute_node_login(compute_node, compute_node_login):
    """Creates a compute node login CV term.

    """
    return create("compute_node_login", compute_node_login)

    # term.compute_node_id = _get_id(db.types.ComputeNode, compute_node)
    # term.login = compute_node_login
    # term.first_name = "UNKNOWN"
    # term.family_name = "UNKNOWN"
    # term.email = "UNKNOWN"


def create_compute_node_machine(compute_node, compute_node_machine):
    """Creates a compute node machine CV term.

    """
    compute_node = compute_node.lower()
    compute_node_machine = compute_node_machine.lower()
    compute_node_machine = "{0}-{1}".format(compute_node, compute_node_machine)

    return create("compute_node_machine", compute_node_machine)

    # term.compute_node_id = _get_id(db.types.ComputeNode, compute_node)


def create_experiment(activity, experiment, experiment_group=_DEFAULT_EXPERIMENT_GROUP):
    """Creates an experiment CV term.

    """
    return create("experiment", experiment)

    # term.activity_id = _get_id(db.types.Activity, activity)
    # term.group_id = _get_id(db.types.ExperimentGroup, experiment_group)



def create_experiment_group(activity, experiment_group):
    """Creates an experiment group CV term.

    """
    return create("experiment_group", experiment_group)

    # term.activity_id = _get_id(db.types.Activity, activity)


def create_model(model, institute=_DEFAULT_INSITUTE):
    """Creates a model CV term.

    """
    model = model.upper()

    return create("model", model)

    # term.institute_id = _get_id(db.types.Institute, institute)


def create_simulation_space(space):
    """Creates a simulation space CV term.

    """
    space = space.upper()

    return create("simulation_space", space)


def create_simulation_state(state):
    """Creates a simulation state CV term.

    """
    state = state.upper()

    return create("simulation_state", state)