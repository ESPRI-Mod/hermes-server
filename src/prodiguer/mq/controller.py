# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.controller.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Message queue system entry point.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from . import utils
from ..utils import (
    config as cfg,  
    convert,
    runtime as rt
    )



# Error messages.
_ERR_MQ_PRODUCER_UNSUPPORTED = "Message queue producer is unsupported."
_ERR_MQ_PRODUCER_ACTION_UNSUPPORTED = "Message queue producer action is unsupported."
_ERR_MQ_CONSUMER_UNSUPPORTED = "Message queue consumer is unsupported."


# Set of consumer mq agents. 
_consumers = {}

# Set of producer mq agents. 
_producers = {}


def _log(q, msg):
    """Logging utilty function."""
    rt.log_mq(msg, cfg.mq.host, q)


def initialise(agents):
    """Initialises set of supported mq agents.

    :param agents: Set of supported mq agents.
    :type agents: dict

    """ 
    for q, agent in agents.items():
        producer, consumer = agent
        _consumers[q] = consumer
        _producers[q] = producer


def produce(q, action_name='start', *args, **kwargs):
    """Opens an mq channel and routes callbacks to the consumer for processing.

    :param q: Key of mq to be consumed.
    :type q: str

    """     
    # Defensive programming.
    if q not in _producers:
        rt.throw(_ERR_MQ_PRODUCER_UNSUPPORTED)
    if not hasattr(_producers[q], action_name):
        rt.throw(_ERR_MQ_PRODUCER_ACTION_UNSUPPORTED)

    # Initiate message production.
    action = getattr(_producers[q], action_name)
    action(*args, **kwargs)


def consume(q):
    """Opens an mq channel and routes callbacks to the consumer for processing.

    :param q: Key of mq to be consumed.
    :type q: str

    """ 
    # Defensive programming.
    if q not in _consumers:
        rt.throw(_ERR_MQ_CONSUMER_UNSUPPORTED)


    def _consume(ch, method, properties, body):
        print "MQ", method
        print "MQ", properties.headers
        print "MQ", body.__class__

        _log("CONSUMING :: {0}".format(body), q)
        try:
            _consumers[q].consume(convert.json_to_dict(body))
        except Exception as e:
            rt.log_mq_error(e)
            if cfg.get_mq_config(q).acknowledge:
                ch.basic_ack(delivery_tag = method.delivery_tag)
        else:
            _log("CONSUMED :: {0}".format(body), q)
            if cfg.get_mq_config(q).acknowledge:
                ch.basic_ack(delivery_tag = method.delivery_tag)


    # Initiate message consumption.
    utils.consume(_consumers[q].MQ, _consume)
