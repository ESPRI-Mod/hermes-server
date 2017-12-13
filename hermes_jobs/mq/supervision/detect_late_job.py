# -*- coding: utf-8 -*-

"""
.. module:: supervisor_detect_late_job.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Detects whether jobs are late and if so initiates a supervision.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from hermes import mq
from hermes.db import pgres as db
from hermes_jobs.mq import utils



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack,
        _verify,
        _persist,
        _enqueue
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True, validate_props=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(props, body, decode=decode, validate_props=validate_props)

        self.job = None
        self.job_uid = None
        self.simulation_uid = None
        self.supervision = None
        self.trigger_code = None


def _unpack(ctx):
    """Unpacks message being processed.

    """
    ctx.job_uid = ctx.content['job_uid']
    ctx.simulation_uid = ctx.content['simulation_uid']
    ctx.trigger_code = ctx.content['trigger_code']


def _verify(ctx):
    """Verification step.

    """
    # Set job.
    ctx.job = db.dao_monitoring.retrieve_job(ctx.job_uid)

    # Abort if job was purged or complete.
    ctx.abort = ctx.job is None or \
                ctx.job.execution_end_date is not None


def _persist(ctx):
    """Persist info to database.

    """
    # Persist job info.
    db.dao_monitoring.persist_late_job(
        ctx.job_uid,
        ctx.simulation_uid
        )

    # Persist supervision info.
    ctx.supervision = db.dao_superviseur.create_supervision(
        ctx.simulation_uid,
        ctx.job_uid,
        ctx.trigger_code
        )


def _enqueue(ctx):
    """Enqueues messages upon downstream queues.

    """
    # Notify front end.
    utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": u"job_late",
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": unicode(ctx.simulation_uid)
    })

    # Initiate supervision.
    utils.enqueue(mq.constants.MESSAGE_TYPE_8100, {
        "job_uid": ctx.job_uid,
        "simulation_uid": ctx.simulation_uid,
        "supervision_id": ctx.supervision.id
    })
