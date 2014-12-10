# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.constants.py
   :platform: Unix
   :synopsis: Prodiguer db constants.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
# Constants pertaining to simulation space.
SIMULATION_SPACE_DEVT = 'DEVT'
SIMULATION_SPACE_FAIL = 'FAIL'
SIMULATION_SPACE_PROD = 'PROD'
SIMULATION_SPACE_TEST = 'TEST'

# Set of supported simulation spaces.
SIMULATION_SPACE_SET = [
    SIMULATION_SPACE_DEVT,
    SIMULATION_SPACE_FAIL,
    SIMULATION_SPACE_PROD,
    SIMULATION_SPACE_TEST
]

# Constants pertaining to simulation states.
SIMULATION_STATE_QUEUED = u"queued"
SIMULATION_STATE_RUNNING = u"running"
SIMULATION_STATE_SUSPENDED = u"suspended"
SIMULATION_STATE_COMPLETE = u"complete"
SIMULATION_STATE_ROLLBACK = u"rollback"
SIMULATION_STATE_ERROR = u"error"

# Set of supported simulation states.
SIMULATION_STATE_SET = [
    SIMULATION_STATE_QUEUED,
    SIMULATION_STATE_RUNNING,
    SIMULATION_STATE_SUSPENDED,
    SIMULATION_STATE_COMPLETE,
    SIMULATION_STATE_ROLLBACK,
    SIMULATION_STATE_ERROR
]
