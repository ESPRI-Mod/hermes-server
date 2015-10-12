# -*- coding: utf-8 -*-

"""
.. module:: monitoring_simulation_end.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring 0100 messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
from prodiguer import mq
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer_jobs.mq import utils



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_message_content,
        _persist_simulation,
        _persist_job,
        _enqueue_front_end_notification
    )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.is_error = props.type == mq.constants.MESSAGE_TYPE_1999
        self.job_uid = None
        self.simulation = None
        self.simulation_uid = None


def _unpack_message_content(ctx):
    """Unpacks message being processed.

    """
    ctx.job_uid = ctx.content['jobuid']
    ctx.simulation_uid = ctx.content['simuid']


def _persist_simulation(ctx):
    """Persists simulation updates to dB.

    """
    ctx.simulation = dao.persist_simulation_02(
        ctx.msg.timestamp,
        ctx.is_error,
        ctx.simulation_uid
        )


def _persist_job(ctx):
    """Persists job updates to dB.

    """
    dao.persist_job_02(
        ctx.msg.timestamp,
        ctx.is_error,
        ctx.job_uid,
        ctx.simulation_uid
        )


def _enqueue_front_end_notification(ctx):
    """Places a message upon the front-end notification queue.

    """
    # Skip if the 0000 message has not yet been received.
    if ctx.simulation.hashid is None:
        return

    # Skip if not the active simulation.
    active_simulation = dao.retrieve_active_simulation(ctx.simulation.hashid)
    if ctx.simulation.uid != active_simulation.uid:
        return

    # Set event type.
    event_type = u"simulation_error" if ctx.is_error else u"simulation_complete"

    # Enqueue notification.
    utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": event_type,
        "simulation_uid": unicode(ctx.simulation_uid)
    })
