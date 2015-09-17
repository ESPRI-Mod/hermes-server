# -*- coding: utf-8 -*-

"""
.. module:: monitoring_job_late.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring job late messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import arrow

from prodiguer import mq
from prodiguer.db import pgres as db
from prodiguer_jobs.mq import utils



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      _unpack_message_content,
      _verify_is_late,
      _enqueue_supervisor_notification
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


def _unpack_message_content(ctx):
    """Unpacks message being processed.

    """
    ctx.job_uid = ctx.content['job_uid']
    ctx.simulation_uid = ctx.content['simulation_uid']


def _verify_is_late(ctx):
    """Verifies that the job is late.

    """
    job = db.dao_monitoring.retrieve_job(ctx.job_uid)
    ctx.abort = job is None or job.execution_end_date is None 


def _enqueue_supervisor_notification(ctx):
    """Places a message upon the supervisor notification queue.

    """
    utils.enqueue(mq.constants.MESSAGE_TYPE_8000, {
        "reason": "late_job",
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": unicode(ctx.simulation_uid)
    })
