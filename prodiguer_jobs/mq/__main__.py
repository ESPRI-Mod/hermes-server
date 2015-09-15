# -*- coding: utf-8 -*-

"""
.. module:: __main__.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
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
from prodiguer_jobs.mq import internal_cv
from prodiguer_jobs.mq import internal_fe
from prodiguer_jobs.mq import internal_smtp
from prodiguer_jobs.mq import internal_smtp_realtime
from prodiguer_jobs.mq import metrics_7000
from prodiguer_jobs.mq import metrics_7100
from prodiguer_jobs.mq import monitoring
from prodiguer_jobs.mq import monitoring_0000
from prodiguer_jobs.mq import monitoring_0100
from prodiguer_jobs.mq import monitoring_9999
from prodiguer_jobs.mq import monitoring_job_end
from prodiguer_jobs.mq import monitoring_job_error
from prodiguer_jobs.mq import monitoring_job_late
from prodiguer_jobs.mq import monitoring_job_start
from prodiguer_jobs.mq import supervisor
from prodiguer_jobs.mq import supervisor_8000
from prodiguer_jobs.mq import supervisor_8100
from prodiguer_jobs.mq import supervisor_8200

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


# Map of agent type keys to corresponding handlers.
_AGENTS = {
    # ... debug agents
    'debug-0000': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_0000
        },
    'debug-0100': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_0100
        },
    'debug-1000': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_job_start
        },
    'debug-1100': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_job_end
        },
    'debug-1199': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY_DELAYED,
        'handler': monitoring_job_late
        },
    'debug-2000': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_job_start
        },
    'debug-2100': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_job_end
        },
    'debug-2199': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY_DELAYED,
        'handler': monitoring_job_late
        },
    'debug-2900': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_job_error
        },
    'debug-3000': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_job_start
        },
    'debug-3100': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_job_end
        },
    'debug-3199': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY_DELAYED,
        'handler': monitoring_job_late
        },
    'debug-3900': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_job_error
        },
    'debug-7000': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': metrics_7000
        },
    'debug-7100': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': metrics_7100
        },
    'debug-8000': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': supervisor_8000
        },
    'debug-8100': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': supervisor_8100
        },
    'debug-8200': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': supervisor_8200
        },
    'debug-9999': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring_9999
        },
    'debug-cv': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': internal_cv
        },
    'debug-fe': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': internal_fe
        },
    'debug-smtp': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': internal_smtp
        },
    'debug-smtp-realtime': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': internal_smtp_realtime
        },
    # ... live agents
    'live-cv': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': internal_cv
        },
    'live-fe': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': internal_fe
        },
    'live-metrics-env': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': metrics_7000
        },
    'live-metrics-sim': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': metrics_7100
        },
    'live-monitoring-compute': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring
        },
    'live-monitoring-post-processing': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_PRIMARY,
        'handler': monitoring
        },
    'live-smtp': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': internal_smtp
        },
    'live-smtp-realtime': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': internal_smtp_realtime
        },
    'live-superviseur': {
        'exchange': mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
        'handler': supervisor
        }
    }


def _get_handler_context_type(agent):
    """Returns an agent handler's processing context type.

    """
    try:
        return agent['handler'].ProcessingContextInfo
    except AttributeError:
        return mq.Message


def _get_handler_tasks(agent):
    """Returns an agent handler's set of message processing tasks.

    """
    try:
        return agent['handler'].get_tasks()
    except AttributeError:
        return []


def _get_handler_error_tasks(agent):
    """Returns an agent handler's set of message error processing tasks.

    """
    try:
        return agent['handler'].get_error_tasks()
    except AttributeError:
        return []


def _process(agent, agent_type, ctx):
    """Processes a message.

    """
    tasks = _get_handler_tasks(agent)
    error_tasks = _get_handler_error_tasks(agent)

    rt.invoke_mq(agent_type, tasks, error_tasks, ctx)


def _execute_agent(agent, agent_type, agent_limit):
    """Executes a standard agent.

    """
    # Start db session.
    db.session.start()

    try:
        # Initialise cv session.
        cv.session.init()

        # Consume messages.
        mq.utils.consume(agent['exchange'],
                         agent.get('queue', agent_type),
                         lambda ctx: _process(agent, agent_type, ctx),
                         consume_limit=agent_limit,
                         context_type=_get_handler_context_type(agent),
                         verbose=agent_limit > 0)
    finally:
        db.session.end()


def _execute(agent_type, agent_limit):
    """Executes message agent.

    """
    # Set agent to be launched.
    try:
        agent = _AGENTS[agent_type]
    except KeyError:
        raise ValueError("Invalid agent type: {0}".format(agent_type))

    # Escape if handler is not yet developed
    if agent['handler'] is None:
        logger.log_mq("Message agent is null: {0}".format(agent_type))
        return

    # Execute.
    logger.log_mq("Launching message agent: {0}".format(agent_type))
    if hasattr(agent['handler'], 'execute'):
        agent['handler'].execute(agent_limit)
    else:
        _execute_agent(agent, agent_type, agent_limit)


# Main entry point.
_execute(options.agent_type, options.agent_limit)
