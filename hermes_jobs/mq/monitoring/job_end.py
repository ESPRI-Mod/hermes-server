# -*- coding: utf-8 -*-

"""
.. module:: monitoring_job_end.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring job end messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
from prodiguer import mq
from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer.db.pgres import dao_superviseur
from hermes_jobs.mq import utils



# Set of message types that correspond to job errors.
_ERROR_MESSAGE_TYPES = {
    mq.constants.MESSAGE_TYPE_1999,     # Compute job fatal error
    mq.constants.MESSAGE_TYPE_2999      # Post-processing job fatal error
}

# Set of message types that require supervision.
_SUPERVISION_MESSAGE_TYPES = {
    mq.constants.MESSAGE_TYPE_1999,     # Compute job fatal error
}

# Set of message types that indicate a simulation end.
_END_SIMULATION_MESSAGE_TYPES = {
    mq.constants.MESSAGE_TYPE_0100,
    mq.constants.MESSAGE_TYPE_1999
}


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _persist,
        _enqueue_supervisor_format,
        _enqueue_fe_notification_job,
        _enqueue_fe_notification_simulation
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.is_compute_end = props.type == mq.constants.MESSAGE_TYPE_0100
        self.is_error = props.type in _ERROR_MESSAGE_TYPES
        self.requires_supervision = props.type in _SUPERVISION_MESSAGE_TYPES
        self.job_uid = None
        self.simulation = None
        self.simulation_uid = None


def _unpack_content(ctx):
    """Unpacks message being processed.

    """
    ctx.job_uid = ctx.content['jobuid']
    ctx.simulation_uid = ctx.content['simuid']


def _persist(ctx):
    """Persists job updates to dB.

    """
    # Persist job info.
    dao.persist_job_end(
        ctx.msg.timestamp,
        ctx.is_compute_end,
        ctx.is_error,
        ctx.job_uid,
        ctx.simulation_uid
        )

    # Persist simulation info.
    if ctx.props.type in _END_SIMULATION_MESSAGE_TYPES:
        ctx.simulation = dao.persist_simulation_end(
            ctx.msg.timestamp,
            ctx.is_error,
            ctx.simulation_uid
            )

    # Persist supervision info.
    if ctx.requires_supervision:
        ctx.supervision = dao_superviseur.create_supervision(
            ctx.simulation_uid,
            ctx.job_uid,
            ctx.props.type
            )

    # Commit to database.
    db.session.commit()


def _enqueue_supervisor_format(ctx):
    """Places a message upon the supervisor format queue.

    """
    if not ctx.requires_supervision:
        return

    utils.enqueue(mq.constants.MESSAGE_TYPE_8100, {
        "job_uid": ctx.job_uid,
        "simulation_uid": ctx.simulation_uid,
        "supervision_id": ctx.supervision.id
    })


def _enqueue_fe_notification_job(ctx):
    """Places a job event message upon the front-end notification queue.

    """
    if ctx.props.type in _END_SIMULATION_MESSAGE_TYPES:
        return

    utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": u"job_error" if ctx.is_error else u"job_complete",
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": unicode(ctx.simulation_uid)
    })


def _enqueue_fe_notification_simulation(ctx):
    """Places a simulation event message upon the front-end notification queue.

    """
    if ctx.props.type not in _END_SIMULATION_MESSAGE_TYPES:
        return

    # Skip if the 0000 message has not yet been received.
    if ctx.simulation.hashid is None:
        return

    # Skip if not the active simulation.
    active_simulation = dao.retrieve_active_simulation(ctx.simulation.hashid)
    if ctx.simulation.uid != active_simulation.uid:
        return

    # Enqueue notification.
    utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": u"simulation_error" if ctx.is_error else u"simulation_complete",
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": unicode(ctx.simulation_uid)
    })

