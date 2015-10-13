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
    mq.constants.MESSAGE_TYPE_1999,     # Compute job fatal error
    mq.constants.MESSAGE_TYPE_2999,     # Post-processing job fatal error
    mq.constants.MESSAGE_TYPE_3999      # Post-processing-from-checker job fatal error
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
        self.is_error = props.type in _JOB_ERROR_MESSAGE_TYPES


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
        ctx.is_error,
        ctx.job_uid,
        ctx.simulation_uid
        )


def enqueue_front_end_notification(ctx):
    """Places a message upon the front-end notification queue.

    """
    utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": u"job_error" if ctx.is_error else u"job_complete",
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": unicode(ctx.simulation_uid)
    })
