# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.utils.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of message queue utilty functions.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>

"""
import uuid

import pika

from . import constants
from .consumer import Consumer
from .producer import Producer
from ..utils import config, convert



class Message(object):
    """Wraps a message either being consumed or produced."""
    def __init__(self, props, content, exchange=None):
        """Constructor.

        :param pika.BasicProperties props: Set of AMPQ properties associated with the message.
        :param object content: Message content.
        :param str exchange: An AMPQ message exchange.

        """
        # Validate inputs.
        if not isinstance(props, pika.BasicProperties):
            raise ValueError("AMPQ message basic properties is invalid.")
        if 'mode' not in props.headers:
            msg = "[mode] is a required header field."
            raise ValueError(msg)
        if props.headers['mode'] not in constants.MODES:
            msg = "Unsupported mode: {0}."
            raise ValueError(msg.format(props.headers['mode']))
        if 'producer_id' not in props.headers:
            msg = "[producer_id] is a required header field."
            raise ValueError(msg)
        if props.headers['producer_id'] not in constants.PRODUCERS:
            msg = "Unsupported producer_id: {0}."
            raise ValueError(msg.format(props.headers['producer_id']))


        self.exchange = exchange
        self.content = content
        self.content_raw = content
        self.content_type = props.content_type
        self.props = props
        self.routing_key = "{0}.{1}.{2}.{3}.{4}".format(props.headers['mode'],
                                                        props.user_id,
                                                        props.headers['producer_id'],
                                                        props.app_id,
                                                        props.type).lower()


    def parse_content(self):
        """Parses message content.

        """
        # Auto convert content to json.
        if self.content_type in (None, constants.CONTENT_TYPE_JSON):
            self.content = convert.dict_to_json(self.content)


def create_ampq_message_properties(
    user_id,
    producer_id,
    app_id,
    message_type,
    message_id=None,
    headers={},
    cluster_id=None,
    content_encoding=constants.DEFAULT_CONTENT_ENCODING,
    content_type=constants.DEFAULT_CONTENT_TYPE,
    delivery_mode = constants.DEFAULT_DELIVERY_MODE,
    expiration=constants.DEFAULT_EXPIRATION,
    mode=constants.DEFAULT_MODE,
    priority=constants.DEFAULT_PRIORITY,
    reply_to=None,
    timestamp=None):
    """Factory function to return set of AMQP message properties.

    :param str user_id: ID of AMPQ user account under which messages are being dispatched.
    :param str producer_id: Message producer identifier.
    :param str app_id: Message application identifier.
    :param str message_type: Message type identifier.
    :param uuid message_id: Message unique identifier.
    :param dict headers: Custom message headers.
    :param str cluster_id: ID of MQ cluster.
    :param str content_encoding: Content encoding, e.g. utf-8.
    :param str content_type: Content MIME type, e.g. application/json.
    :param int delivery_mode: Message delivery mode (2 = with acknowledgement).
    :param int expiration: Ticks until message will no be considered as active.
    :param str mode: Messaging mode (dev|test|prod).
    :param int priority: Messaging priority.
    :param str reply_to: Messaging RPC callback.
    :param str timestamp: Timestamp.

    :returns pika.BasicProperties: Set of AMPQ message basic properties.

    """
    # Validate inputs.
    if producer_id not in constants.PRODUCERS:
        msg = "Unsupported producer identifier: {0}."
        raise ValueError(msg.format(producer_id))
    if app_id not in constants.APPS:
        msg = "Unsupported application identifier: {0}."
        raise ValueError(msg.format(app_id))
    if message_type not in constants.TYPES:
        msg = "Unsupported message type identifier: {0}."
        raise ValueError(msg.format(message_type))
    if user_id not in constants.USERS:
        msg = "Unsupported user identifier: {0}."
        raise ValueError(msg.format(user_id))
    if content_encoding not in constants.CONTENT_ENCODINGS:
        msg = "Unsupported content encoding: {0}."
        raise ValueError(msg.format(content_encoding))
    if content_type not in constants.CONTENT_TYPES:
        msg = "Unsupported content type: {0}."
        raise ValueError(msg.format(content_type))
    if delivery_mode not in constants.AMPQ_DELIVERY_MODES:
        msg = "Unsupported delivery mode: {0}."
        raise ValueError(msg.format(delivery_mode))
    if mode not in constants.MODES:
        msg = "Unsupported mode: {0}."
        raise ValueError(msg.format(mode))
    if priority not in constants.PRIORITIES:
        msg = "Unsupported priority: {0}."
        raise ValueError(msg.format(priority))

    # Format inputs.
    if message_id is None:
        message_id = unicode(uuid.uuid4())
    if timestamp is None:
        timestamp = convert.now_to_timestamp()

    # Default headers attached with each property.
    default_headers = {
        "mode": mode,
        "producer_id": producer_id
    }

    # Return a pika BasicProperties instance (follows AMPQ protocol).
    return pika.BasicProperties(
        app_id=app_id,
        cluster_id=cluster_id,
        content_type=content_type,
        content_encoding=content_encoding,
        correlation_id=None,
        delivery_mode = delivery_mode,
        expiration=expiration,
        headers=dict(default_headers.items() + headers.items()),
        message_id=message_id,
        priority=priority,
        reply_to=reply_to,
        timestamp=timestamp,
        type=message_type,
        user_id=user_id
        )


def publish(msg_source,
            connection_url=None,
            enable_confirmations=True,
            publish_limit=constants.DEFAULT_PUBLISH_LIMIT,
            publish_interval=constants.DEFAULT_PUBLISH_INTERVAL,
            verbose=False):
    """Publishes message(s) to MQ server.

    :param msg_source: Source of messages for publishing.
    :type msg_source: Message | function
    :param str connection_url: An MQ server connection URL.
    :param int connection_reopen_delay: Delay in seconds before a connection is reopened after somekind of issue.
    :param bool enable_confirmations: Flag indicating whether message delivery confirmations are required.
    :param int publish_limit: Maximum number of message publishing events.
    :param int publish_interval: Frequency at which message(s) are published.
    :param bool verbose: Flag indicating whether logging level is verbose or not.

    """
    # Override defaults from config.
    if connection_url is None:
        connection_url=config.mq.connections.main

    # Instantiate producer.
    producer = Producer(msg_source,
                        connection_url,
                        enable_confirmations,
                        publish_limit,
                        publish_interval,
                        verbose)

    # Run.
    try:
        producer.run()
    except KeyboardInterrupt:
        producer.stop()


produce = publish


def consume(exchange,
            queue,
            callback,
            connection_url=None,
            consume_limit=0,
            verbose=False):
    """Consumes message(s) from an MQ server.

    :param str exchange: Name of an exchange to bind to.
    :param str queue: Name of queue to bind to.
    :param func callback: Function to invoke when message has been handled.
    :param str connection_url: An MQ server connection URL.
    :param int connection_reopen_delay: Delay in seconds before a connection is reopened after somekind of issue.
    :param int consume_limit: Limit upon number of message to be consumed.
    :param bool verbose: Flag indicating whether logging level is verbose or not.

    """
    # Override defaults from config.
    if connection_url is None:
        connection_url=config.mq.connections.main

    # Instantiate producer.
    consumer = Consumer(exchange,
                        queue,
                        callback,
                        connection_url,
                        consume_limit,
                        verbose)

    # Run.
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()