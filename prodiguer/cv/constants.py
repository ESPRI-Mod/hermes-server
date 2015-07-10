# -*- coding: utf-8 -*-

"""
.. module:: cv.constants.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary constants.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
# Term governance states.
TERM_GOVERNANCE_STATE_NEW = "new"
TERM_GOVERNANCE_STATE_APPROVED = "approved"
TERM_GOVERNANCE_STATE_REJECTED = "rejected"
TERM_GOVERNANCE_STATE_DELETED = "deleted"
TERM_GOVERNANCE_STATE_DESTROYED = "destroyed"

# Set of term governance states.
TERM_GOVERNANCE_STATESET = [
    TERM_GOVERNANCE_STATE_NEW,
    TERM_GOVERNANCE_STATE_APPROVED,
    TERM_GOVERNANCE_STATE_REJECTED
    ]

# Term types.
TERM_TYPE_ACTIVITY = u"activity"
TERM_TYPE_INSTITUTE = u"institute"
TERM_TYPE_COMPUTE_NODE = u"compute_node"
TERM_TYPE_COMPUTE_NODE_LOGIN = u"compute_node_login"
TERM_TYPE_COMPUTE_NODE_MACHINE = u"compute_node_machine"
TERM_TYPE_EXPERIMENT = u"experiment"
TERM_TYPE_EXPERIMENT_GROUP = u"experiment_group"
TERM_TYPE_JOB_TYPE = u"job_type"
TERM_TYPE_MESSAGE_APPLICATION = u"message_application"
TERM_TYPE_MESSAGE_PRODUCER = u"message_producer"
TERM_TYPE_MESSAGE_TYPE = u"message_type"
TERM_TYPE_MESSAGE_USER = u"message_user"
TERM_TYPE_MODEL = u"model"
TERM_TYPE_MODEL_FORCING = u"model_forcing"
TERM_TYPE_SIMULATION_SPACE = u"simulation_space"
TERM_TYPE_SIMULATION_STATE = u"simulation_state"

# Term typeset.
TERM_TYPESET = [
    TERM_TYPE_ACTIVITY,
    TERM_TYPE_INSTITUTE,
    TERM_TYPE_COMPUTE_NODE,
    TERM_TYPE_COMPUTE_NODE_LOGIN,
    TERM_TYPE_COMPUTE_NODE_MACHINE,
    TERM_TYPE_EXPERIMENT,
    TERM_TYPE_EXPERIMENT_GROUP,
    TERM_TYPE_JOB_TYPE,
    TERM_TYPE_MESSAGE_TYPE,
    TERM_TYPE_MESSAGE_APPLICATION,
    TERM_TYPE_MESSAGE_PRODUCER,
    TERM_TYPE_MESSAGE_USER,
    TERM_TYPE_MODEL,
    TERM_TYPE_MODEL_FORCING,
    TERM_TYPE_SIMULATION_SPACE,
    TERM_TYPE_SIMULATION_STATE
]

# Case sensitive term typeset.
CASE_SENSITIVE_TERM_TYPESET = [
    TERM_TYPE_EXPERIMENT
]

# Constants pertaining to job types.
JOB_TYPE_COMPUTING = u"computing"
JOB_TYPE_POST_PROCESSING = u"post-processing"
JOB_TYPE_POST_PROCESSING_FROM_CHECKER = u"post-processing-from-checker"