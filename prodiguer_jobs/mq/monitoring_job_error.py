# -*- coding: utf-8 -*-

"""
.. module:: monitoring_job_error.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring job fail messages.

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
      _persist_job_updates,
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

        self.job = None
        self.job_uid = None
        self.simulation = None
        self.simulation_uid = None


def _unpack_message_content(ctx):
    """Unpacks message being processed.

    """
    ctx.job_uid = ctx.content['jobuid']
    ctx.simulation_uid = ctx.content['simuid']


def _persist_job_updates(ctx):
    """Persists job updates to dB.

    """
    # Retrieve job.
    job = dao.retrieve_job(ctx.job_uid)

    # Escape if job already in error state.
    if job and job.is_error:
        ctx.abort = True
        return

    # Update job.
    ctx.job = dao.persist_job_02(
        ctx.msg.timestamp,
        True,
        ctx.job_uid,
        ctx.simulation_uid
        )


def _enqueue_front_end_notification(ctx):
    """Places a message upon the front-end notification queue.

    """
    # Skip if job already in error state.
    if ctx.job is None:
        return

    # Skip if simulation start (0000) message not received.
    simulation = dao.retrieve_simulation(ctx.simulation_uid)
    if simulation is None:
        return

    # Skip if simulation is obsolete (i.e. it was restarted).
    if simulation.is_obsolete:
        return

    utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": u"job_error",
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": unicode(ctx.simulation_uid)
    })
