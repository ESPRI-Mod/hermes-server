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

from tornado.options import define, options
from prodiguer import cv
from prodiguer import mq
from prodiguer.utils import logger
from prodiguer.utils import rt
from prodiguer.db import pgres as db

import ext_smtp
import ext_smtp_realtime
import in_metrics_env
import in_metrics_sim
import in_monitoring
import in_monitoring_0000
import in_monitoring_0100
import in_monitoring_1000
import in_monitoring_1100
import in_monitoring_2000
import in_monitoring_2100
import in_monitoring_2900
import in_monitoring_3000
import in_monitoring_3100
import in_monitoring_3900
import in_monitoring_4000
import in_monitoring_4100
import in_monitoring_4900
import in_monitoring_8888
import in_monitoring_9000
import in_monitoring_9999
import internal_api
import internal_cv



# Set logging options.
logging.getLogger("pika").setLevel(logging.ERROR)
logging.getLogger("requests").setLevel(logging.ERROR)


# Map of agent type keys to agent handlers.
_AGENTS = {
    # Live agents.
    'ext-smtp': ext_smtp,
    'ext-smtp-realtime': ext_smtp_realtime,
    'internal-api': internal_api,
    'internal-cv': internal_cv,
    'in-monitoring-compute': in_monitoring,
    'in-monitoring-post-processing': in_monitoring,
    'in-metrics-env': in_metrics_env,
    'in-metrics-sim': in_metrics_sim,
    # Debug agents.
    'debug-ext-smtp': ext_smtp,
    'debug-in-metrics-env': in_metrics_env,
    'debug-in-metrics-sim': in_metrics_sim,
    'debug-in-monitoring-0000': in_monitoring_0000,
    'debug-in-monitoring-0100': in_monitoring_0100,
    'debug-in-monitoring-1000': in_monitoring_1000,
    'debug-in-monitoring-1100': in_monitoring_1100,
    'debug-in-monitoring-2000': in_monitoring_2000,
    'debug-in-monitoring-2100': in_monitoring_2100,
    'debug-in-monitoring-2900': in_monitoring_2900,
    'debug-in-monitoring-3000': in_monitoring_3000,
    'debug-in-monitoring-3100': in_monitoring_3100,
    'debug-in-monitoring-3900': in_monitoring_3900,
    'debug-in-monitoring-4000': in_monitoring_4000,
    'debug-in-monitoring-4100': in_monitoring_4100,
    'debug-in-monitoring-4900': in_monitoring_4900,
    'debug-in-monitoring-8888': in_monitoring_8888,
    'debug-in-monitoring-9000': in_monitoring_9000,   # TODO - deprecate
    'debug-in-monitoring-9999': in_monitoring_9999,
    'debug-internal-api': internal_api,
    'debug-internal-cv': internal_cv,
}


def _get_queue(agent_type):
    """Returns queue that an agent is related to.

    """
    return "q-{0}".format(agent_type)


def _get_exchange(agent_type):
    """Returns exchange that an agent is related to.

    """
    parts = agent_type.split('-')

    return "x-{}".format(parts[1] if parts[0] == 'debug' else parts[0])


def _get_consumer_context_type(agent):
    """Returns a consumers processing context type.

    """
    try:
        return agent.ProcessingContextInfo
    except AttributeError:
        return mq.Message


def _get_agent_tasks(agent):
    """Returns an agent's set of message processing tasks.

    """
    try:
        return agent.get_tasks()
    except AttributeError:
        return []


def _get_agent_error_tasks(agent):
    """Returns an agent's set of message error processing tasks.

    """
    try:
        return agent.get_error_tasks()
    except AttributeError:
        return None


def _process(agent, agent_type, ctx):
    """Processes a message.

    """
    rt.invoke_mq(agent_type,
                 _get_agent_tasks(agent),
                 error_tasks=_get_agent_error_tasks(agent),
                 ctx=ctx)


def _execute_agent(agent, agent_type, agent_limit):
    """Executes a standard agent.

    """
    # Start db session.
    db.session.start()

    try:
        # Initialise cv session.
        cv.session.init()

        # Consume messages.
        mq.utils.consume(_get_exchange(agent_type),
                         _get_queue(agent_type),
                         lambda ctx: _process(agent, agent_type, ctx),
                         consume_limit=agent_limit,
                         context_type=_get_consumer_context_type(agent),
                         verbose=agent_limit > 0)
    finally:
        db.session.end()


def _execute(agent_type, agent_limit):
    """Executes message agent.

    """
    # Strip irrelevant agent type prefixes.
    if agent_type.startswith('q-'):
        agent_type = agent_type[2:]

    # Set agent to be launched.
    if agent_type not in _AGENTS:
        raise ValueError("Invalid agent type: {0}".format(agent_type))
    agent = _AGENTS[agent_type]

    # Log.
    logger.log_mq("Launching message agent: {0}".format(agent_type))

    # Execute.
    if hasattr(agent, 'execute'):
        agent.execute(agent_limit)
    else:
        _execute_agent(agent, agent_type, agent_limit)


# Define command line arguments.
define("agent_type",
       help="Type of message agent to launch")
define("agent_limit",
       default=0,
       help="Agent limit (0 = unlimited)",
       type=int)
options.parse_command_line()

# Main entry point.
_execute(options.agent_type, options.agent_limit)
