# -*- coding: utf-8 -*-

"""
.. module:: monitoring_job_end.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring job end messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer_jobs.mq import utils


# Set of message type that correspond to job errors.
_JOB_ERROR_MESSAGE_TYPES = {
    mq.constants.MESSAGE_TYPE_2900,
    mq.constants.MESSAGE_TYPE_3900,
    mq.constants.MESSAGE_TYPE_9999
}


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      unpack_message_content,
      persist_job,
      enqueue_front_end_notification
      )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.job_uid = None
        self.simulation_uid = None


def unpack_message_content(ctx):
    """Unpacks message being processed.

    """
    ctx.job_uid = ctx.content['jobuid']
    ctx.simulation_uid = ctx.content['simuid']


def persist_job(ctx):
    """Persists job updates to dB.

    """
    dao.persist_job_02(
        ctx.msg.timestamp,
        ctx.props.type in _JOB_ERROR_MESSAGE_TYPES,
        ctx.job_uid,
        ctx.simulation_uid
        )


def enqueue_front_end_notification(ctx):
    """Places a message upon the front-end notification queue.

    """
    # Skip if simulation start (0000) message not received.
    simulation = dao.retrieve_simulation(ctx.simulation_uid)
    if simulation is None:
        return

    # Skip if simulation is obsolete (i.e. it was restarted).
    if simulation.is_obsolete:
        return

    # Set front-end event type.    
    if ctx.props.type in _JOB_ERROR_MESSAGE_TYPES:
        event_type = u"job_error"
    else:
        event_type = u"job_complete"

    utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": event_type,
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": unicode(ctx.simulation_uid)
    })
