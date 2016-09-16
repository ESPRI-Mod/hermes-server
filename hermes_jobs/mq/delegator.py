# -*- coding: utf-8 -*-

"""
.. module:: delegator.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: A message sub-handler that delegates message processing to other actual handlers.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from hermes_jobs.mq import metrics
from hermes_jobs.mq import monitoring
from hermes_jobs.mq import supervision
from hermes_jobs.mq.utils import invoke as invoke_handler



# Map of message type to agents.
_AGENTS = {
    # ... monitoring handlers
    '0000': monitoring.job_start,
    '0100': monitoring.job_end,
    '1000': monitoring.job_start,
    '1001': monitoring.job_update,
    '1100': monitoring.job_end,
    '1999': monitoring.job_end,
    '2000': monitoring.job_start,
    '2100': monitoring.job_end,
    '2999': monitoring.job_end,
    '8888': monitoring.simulation_delete,
    # ... metrics handlers
    '7000': metrics.environment,
    '7010': metrics.conso_project,
    '7011': metrics.conso_jobs,
    # ... supervisor handlers
    '8000': supervision.detect_late_job,
    '8100': supervision.format_script,
    '8200': supervision.dispatch_script
}


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return _process


def _get_agent_context_type(agent):
    """Returns an agent handler's processing context type.

    """
    try:
        return agent.ProcessingContextInfo
    except AttributeError:
        return mq.Message


def _process(ctx):
    """Processes a simulation monitoring message pulled from message queue.

    """
    # Ensure message content is decoded.
    ctx.decode()

    # Set sub-agent.
    agent = _AGENTS[ctx.props.type]

    # Set sub-context.
    sub_ctx_type = _get_agent_context_type(agent)
    sub_ctx = sub_ctx_type(ctx.props, ctx.content, decode=False)
    sub_ctx.msg = ctx.msg

    # Invoke tasks.
    invoke_handler(ctx.props.type,
                   agent.get_tasks(),
                   agent.get_error_tasks() if hasattr(agent, "get_error_tasks") else [],
                   sub_ctx)
