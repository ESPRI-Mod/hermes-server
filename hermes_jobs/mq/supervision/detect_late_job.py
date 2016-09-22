# -*- coding: utf-8 -*-

"""
.. module:: supervisor_detect_late_job.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Detects whether jobs are late and if so initiates a supervision.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from prodiguer.db import pgres as db
from hermes_jobs.mq import utils



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _verify,
        _persist_supervision,
        _enqueue_supervisor_format
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(props, body, decode=decode)

        self.job_uid = None
        self.simulation_uid = None
        self.supervision = None
        self.trigger_code = None


def _unpack_content(ctx):
    """Unpacks message being processed.

    """
    ctx.job_uid = ctx.content['job_uid']
    ctx.simulation_uid = ctx.content['simulation_uid']
    ctx.trigger_code = ctx.content['trigger_code']


def _verify(ctx):
    """Verifies that the late job detection is valid.

    """
    job = db.dao_monitoring.retrieve_job(ctx.job_uid)
    ctx.abort = job is None or job.execution_end_date is not None


def _persist_supervision(ctx):
    """Persist supervision info to database.

    """
    ctx.supervision = db.dao_superviseur.create_supervision(
        ctx.simulation_uid,
        ctx.job_uid,
        ctx.trigger_code
        )


def _enqueue_supervisor_format(ctx):
    """Place a message upon the supervisor format queue.

    """
    utils.enqueue(mq.constants.MESSAGE_TYPE_8100, {
        "job_uid": ctx.job_uid,
        "simulation_uid": ctx.simulation_uid,
        "supervision_id": ctx.supervision.id
    })
