# -*- coding: utf-8 -*-

"""
.. module:: __main__.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Main entry point for launching message agents.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import logging

from prodiguer import cv
from prodiguer import mq
from prodiguer.utils import logger
from prodiguer.utils import rt
from prodiguer.db import pgres as db
from prodiguer_jobs.mq import delegator
from prodiguer_jobs.mq import internal_cv
from prodiguer_jobs.mq import internal_fe
from prodiguer_jobs.mq import internal_smtp
from prodiguer_jobs.mq import internal_smtp_checker
from prodiguer_jobs.mq import internal_smtp_realtime
from prodiguer_jobs.mq import metrics_conso
from prodiguer_jobs.mq import metrics_environment
from prodiguer_jobs.mq import metrics_pcmdi
from prodiguer_jobs.mq import monitoring_command_fail
from prodiguer_jobs.mq import monitoring_job_end
from prodiguer_jobs.mq import monitoring_job_start
from prodiguer_jobs.mq import supervisor_detect_late_job
from prodiguer_jobs.mq import supervisor_dispatch_script
from prodiguer_jobs.mq import supervisor_format_script

from tornado.options import define
from tornado.options import options



# Define command line arguments.
define("agent_type",
       help="Type of message agent to launch")
define("agent_limit",
       default=0,
       help="Agent limit (0 = unlimited)",
       type=int)
options.parse_command_line()


# Set logging options.
logging.getLogger("pika").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)


# Map of MQ agents to MQ handlers.
_AGENT_HANDLERS = {
    'debug-0000': monitoring_job_start,
    'debug-0100': monitoring_job_end,
    'debug-1000': monitoring_job_start,
    'debug-1100': monitoring_job_end,
    'debug-1900': monitoring_command_fail,
    'debug-1999': monitoring_job_end,
    'debug-2000': monitoring_job_start,
    'debug-2100': monitoring_job_end,
    'debug-2900': monitoring_command_fail,
    'debug-2999': monitoring_job_end,
    'debug-3000': monitoring_job_start,
    'debug-3100': monitoring_job_end,
    'debug-3900': monitoring_command_fail,
    'debug-3999': monitoring_job_end,
    'debug-7000': metrics_environment,
    'debug-7010': metrics_conso,
    'debug-7100': metrics_pcmdi,
    'debug-8000': supervisor_detect_late_job,
    'debug-8100': supervisor_format_script,
    'debug-8200': supervisor_dispatch_script,
    'debug-8888': None,
    'debug-cv': internal_cv,
    'debug-fe': internal_fe,
    'debug-smtp': internal_smtp,
    'debug-smtp-checker': internal_smtp_checker,
    'debug-smtp-realtime': internal_smtp_realtime,
    'live-cv': internal_cv,
    'live-fe': internal_fe,
    'live-metrics': delegator,
    'live-metrics-pcmdi': metrics_pcmdi,
    'live-monitoring-compute': delegator,
    'live-monitoring-post-processing': delegator,
    'live-smtp': internal_smtp,
    'live-smtp-checker': internal_smtp_checker,
    'live-smtp-realtime': internal_smtp_realtime,
    'live-superviseur': delegator
}

# Map of MQ exchanges to MQ agents.
_AGENT_EXCHANGES = {
    mq.constants.EXCHANGE_PRODIGUER_PRIMARY: {
        'debug-0000',
        'debug-0100',
        'debug-1000',
        'debug-1100',
        'debug-1900',
        'debug-1999',
        'debug-2000',
        'debug-2100',
        'debug-2900',
        'debug-2999',
        'debug-3000',
        'debug-3100',
        'debug-3900',
        'debug-3999',
        'debug-7000',
        'debug-7010',
        'debug-7100',
        'debug-8888',
        'live-metrics',
        'live-metrics-pcmdi',
        'live-monitoring-compute',
        'live-monitoring-post-processing'
    },
    mq.constants.EXCHANGE_PRODIGUER_SECONDARY: {
        'debug-8100',
        'debug-8200',
        'debug-cv',
        'debug-fe',
        'debug-smtp',
        'live-cv',
        'live-fe',
        'live-smtp',
        'live-superviseur'
    },
    mq.constants.EXCHANGE_PRODIGUER_SECONDARY_DELAYED: {
        'debug-8000',
    }
}


def _get_handler_context_type(handler):
    """Returns an agent handler's processing context type.

    """
    try:
        return handler.ProcessingContextInfo
    except AttributeError:
        return mq.Message


def _get_exchange(agent_type):
    """Returns MQ exchange to which to bind.

    """
    for exchange, agent_types in _AGENT_EXCHANGES.items():
        if agent_type in agent_types:
            return exchange

    raise ValueError("Agent cannot be mapped to exchange: {}".format(agent_type))


def _get_queue(agent_type):
    """Returns MQ queue to which to bind.

    """
    return agent_type


def _get_handler_tasks(handler):
    """Returns a handler's set of processing tasks.

    """
    try:
        task_factory = handler.get_tasks
    except AttributeError:
        return []
    else:
        return task_factory()


def _get_handler_error_tasks(handler):
    """Returns a handler's set of error processing tasks.

    """
    try:
        task_factory = handler.get_error_tasks
    except AttributeError:
        return []
    else:
        return task_factory()


def _process(agent_type, handler, ctx):
    """Processes a message.

    """
    rt.invoke_mq(agent_type,
                 _get_handler_tasks(handler),
                 _get_handler_error_tasks(handler),
                 ctx)


def _execute_agent(agent_type, agent_limit, handler):
    """Executes a standard agent.

    """
    # Initiate a db session.
    with db.session.create():

        # Initialise cv session.
        cv.session.init()

        # Consume messages.
        mq.utils.consume(_get_exchange(agent_type),
                         _get_queue(agent_type),
                         lambda ctx: _process(agent_type, handler, ctx),
                         consume_limit=agent_limit,
                         context_type=_get_handler_context_type(handler),
                         verbose=agent_limit > 0)


def _execute(agent_type, agent_limit):
    """Executes message agent.

    """
    # Set handler.
    try:
        handler = _AGENT_HANDLERS[agent_type]
    except KeyError:
        raise ValueError("Invalid agent type: {0}".format(agent_type))

    # Execute.
    logger.log_mq("Launching message agent: {0}".format(agent_type))
    if hasattr(handler, 'execute'):
        handler.execute(agent_limit)
    else:
        _execute_agent(agent_type, agent_limit, handler)


# Main entry point.
_execute(options.agent_type, options.agent_limit)
