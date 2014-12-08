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

# Constants pertaining to execution states.
EXECUTION_STATE_QUEUED = u"queued"
EXECUTION_STATE_RUNNING = u"running"
EXECUTION_STATE_SUSPENDED = u"suspended"
EXECUTION_STATE_COMPLETE = u"complete"
EXECUTION_STATE_ROLLBACK = u"rollback"
EXECUTION_STATE_ERROR = u"error"

# Set of supported execution states.
EXECUTION_STATE_SET = [
    EXECUTION_STATE_QUEUED,
    EXECUTION_STATE_RUNNING,
    EXECUTION_STATE_SUSPENDED,
    EXECUTION_STATE_COMPLETE,
    EXECUTION_STATE_ROLLBACK,
    EXECUTION_STATE_ERROR
]
