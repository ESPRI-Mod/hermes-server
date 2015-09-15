# -*- coding: utf-8 -*-

"""
.. module:: monitoring_job_start.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring job start messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import cv
from prodiguer import mq
from prodiguer.db import pgres as db
from prodiguer.utils import config
from prodiguer_jobs.mq import utils



# Map of job types to warning delay message types.
_WARNING_DELAY_MESSAGE_TYPES = {
    cv.constants.JOB_TYPE_COMPUTING: mq.constants.MESSAGE_TYPE_1199,
    cv.constants.JOB_TYPE_POST_PROCESSING: mq.constants.MESSAGE_TYPE_2199,
    cv.constants.JOB_TYPE_POST_PROCESSING_FROM_CHECKER: mq.constants.MESSAGE_TYPE_3199
}

# Map of message to job types.
_MESSAGE_JOB_TYPES = {
    mq.constants.MESSAGE_TYPE_1000: cv.constants.JOB_TYPE_COMPUTING,
    mq.constants.MESSAGE_TYPE_2000: cv.constants.JOB_TYPE_POST_PROCESSING,
    mq.constants.MESSAGE_TYPE_3000: cv.constants.JOB_TYPE_POST_PROCESSING_FROM_CHECKER
}


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return [
        _drop_obsoletes,
        _unpack_message_content,
        _persist_job,
        _persist_simulation_updates,
        _enqueue_job_warning_delay,
        _enqueue_front_end_notification
    ]


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.accounting_project = None
        self.job_type = _MESSAGE_JOB_TYPES[self.props.type]
        self.job_uid = None
        self.job_warning_delay = None
        self.simulation_uid = None
        self.pp_name = None
        self.pp_date = None
        self.pp_dimension = None
        self.pp_component = None
        self.pp_file = None


def _drop_obsoletes(ctx):
    """Drops messages considered obsolete.

    """
    # If the field 'command' exists then this was
    # from a version of libIGCM now considered obsolete.
    if "command" in ctx.content:
        ctx.abort = True


def _unpack_message_content(ctx):
    """Unpacks message being processed.

    """
    ctx.accounting_project = ctx.content.get('accountingProject')
    ctx.job_uid = ctx.content['jobuid']
    ctx.job_warning_delay = ctx.content.get(
        'jobWarningDelay', config.apps.monitoring.defaultJobWarningDelayInSeconds)
    ctx.simulation_uid = ctx.content['simuid']
    ctx.pp_name = ctx.content.get('postProcessingName')
    ctx.pp_date = ctx.content.get('postProcessingDate')
    ctx.pp_dimension = ctx.content.get('postProcessingDimn')
    ctx.pp_component = ctx.content.get('postProcessingComp')
    ctx.pp_file = ctx.content.get('postProcessingFile')


def _persist_job(ctx):
    """Persists job info to db.

    """
    db.dao_monitoring.persist_job_01(
        ctx.accounting_project,
        ctx.job_warning_delay,
        ctx.msg.timestamp,
        ctx.job_type,
        ctx.job_uid,
        ctx.simulation_uid,
        post_processing_name = ctx.pp_name,
        post_processing_date = ctx.pp_date,
        post_processing_dimension = ctx.pp_dimension,
        post_processing_component = ctx.pp_component,
        post_processing_file = ctx.pp_file
        )


def _persist_simulation_updates(ctx):
    """Updates simulation (compute jobs only)

    """
    # Skip if not processing a compute job.
    if ctx.job_type != cv.constants.JOB_TYPE_COMPUTING:
        return

    # Ensure simulation is not considered to be in an error state.
    db.dao_monitoring.persist_simulation_02(
        None,
        False,
        ctx.simulation_uid
        )


def _enqueue_job_warning_delay(ctx):
    """Places a delayed message indicating the amount of time before the job is considered to be late.

    """
    utils.enqueue(
        _WARNING_DELAY_MESSAGE_TYPES[ctx.job_type],
        # delay_in_ms = ctx.job_warning_delay * 1000,
        delay_in_ms = 5000,
        payload={
            "simulation_uid": ctx.simulation_uid,
            "job_uid": ctx.job_uid
        }
    )


def _enqueue_front_end_notification(ctx):
    """Places a message upon the front-end notification queue.

    """
    # Skip if simulation start (0000) message not received.
    simulation = db.dao_monitoring.retrieve_simulation(ctx.simulation_uid)
    if simulation is None:
        return

    # Skip if simulation is obsolete (i.e. it was restarted).
    if simulation.is_obsolete:
        return

    utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": u"job_start",
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": unicode(ctx.simulation_uid)
        })
