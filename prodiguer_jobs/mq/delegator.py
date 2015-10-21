# -*- coding: utf-8 -*-

"""
.. module:: delegator.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: A message sub-handler that delegates message processing to other actual handlers.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import rt
from prodiguer_jobs.mq import monitoring_command_fail
from prodiguer_jobs.mq import monitoring_job_end
from prodiguer_jobs.mq import monitoring_job_start
from prodiguer_jobs.mq import supervisor_detect_late_job
from prodiguer_jobs.mq import supervisor_dispatch_script
from prodiguer_jobs.mq import supervisor_format_script



# Map of message type to agents.
_AGENTS = {
    # ... monitoring handlers
    '0000': monitoring_job_start,
    '0100': monitoring_job_end,
    '1999': monitoring_job_end,
    '1000': monitoring_job_start,
    '1100': monitoring_job_end,
    '1900': monitoring_command_fail,
    '2000': monitoring_job_start,
    '2100': monitoring_job_end,
    '2900': monitoring_command_fail,
    '2999': monitoring_job_end,
    '3000': monitoring_job_start,
    '3100': monitoring_job_end,
    '3900': monitoring_command_fail,
    '3999': monitoring_job_end,
    # ... supervisor handlers
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
    agent = _AGENTS[ctx.props.type]

    # Set sub-context.
    sub_ctx = agent.ProcessingContextInfo(ctx.props, ctx.content, decode=False)
    sub_ctx.msg = ctx.msg

    # Invoke tasks.
    rt.invoke_mq(ctx.props.type,
                 agent.get_tasks(),
                 agent.get_error_tasks() if hasattr(agent, "get_error_tasks") else [],
                 sub_ctx)
