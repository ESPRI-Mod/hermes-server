# -*- coding: utf-8 -*-

"""
.. module:: monitoring.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Processes simulation monitoring related messages (by delegation).

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import rt
from prodiguer_jobs.mq import supervisor_detect_late_job
from prodiguer_jobs.mq import supervisor_dispatch_script
from prodiguer_jobs.mq import supervisor_format_script



# Map of sub-consumer types to sub-consumers.
_SUB_AGENTS = {
    '8000': supervisor_detect_late_job,
    '8100': supervisor_format_script,
    '8200': supervisor_dispatch_script
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
