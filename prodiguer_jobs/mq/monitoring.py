# -*- coding: utf-8 -*-

"""
.. module:: monitoring.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Processes simulation monitoring related messages (by delegation).

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import rt
from prodiguer_jobs.mq import monitoring_0000
from prodiguer_jobs.mq import monitoring_0100
from prodiguer_jobs.mq import monitoring_9999
from prodiguer_jobs.mq import monitoring_job_end
from prodiguer_jobs.mq import monitoring_job_error
from prodiguer_jobs.mq import monitoring_job_late
from prodiguer_jobs.mq import monitoring_job_start



# Map of sub-consumer types to sub-consumers.
_SUB_AGENTS = {
    # Simulation messages.
    '0000': monitoring_0000,
    '0100': monitoring_0100,
    '9999': monitoring_9999,
    # Computing job messages.
    '1000': monitoring_job_start,
    '1100': monitoring_job_end,
    '1199': monitoring_job_late,
    # Post processing job messages.
    '2000': monitoring_job_start,
    '2100': monitoring_job_end,
    '2199': monitoring_job_late,
    '2900': monitoring_job_error,
    # Post processing from checker job messages.
    '3000': monitoring_job_start,
    '3100': monitoring_job_end,
    '3199': monitoring_job_late,
    '3900': monitoring_job_error
}


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return _process


def _process(ctx):
    """Processes a simulation monitoring message pulled from message queue.

    """
    # Decode message content.
    ctx.decode()

    # Set sub-agent.
    agent = _SUB_AGENTS[ctx.props.type]

    # Set sub-context.
    sub_ctx = agent.ProcessingContextInfo(ctx.props, ctx.content, decode=False)
    sub_ctx.msg = ctx.msg

    # Set tasks to be invoked.
    tasks = agent.get_tasks()
    try:
        error_tasks = agent.get_error_tasks()
    except AttributeError:
        error_tasks = []

    # Invoke tasks.
    rt.invoke_mq(ctx.props.type, tasks, error_tasks, sub_ctx)
