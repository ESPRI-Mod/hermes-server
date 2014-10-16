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

import arrow, pika
from sqlalchemy.exc import IntegrityError

from . import constants, message
from .consumer import Consumer
from .producer import Producer
from .timestamp import Timestamp
from .. import db
from ..utils import convert, rt



def create_ampq_message_properties(
    user_id,
    producer_id,
    app_id,
    message_type,
    message_id=None,
    headers=None,
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

    # Override null inputs.
    if headers is None:
        headers = {}
    if message_id is None:
        message_id = unicode(uuid.uuid4())

    # Set timestamps.
    if timestamp is None:
        timestamp = arrow.now('Europe/Paris')
        headers['timestamp'] = unicode(timestamp)
        headers['timestamp_precision'] = 'ms'
        timestamp = int(repr(timestamp.float_timestamp).replace(".", ""))
    if 'timestamp' not in headers:
        headers['timestamp'] = unicode(arrow.now('Europe/Paris'))
        headers['timestamp_precision'] = 'ms'
    if 'timestamp_precision' not in headers:
        headers['timestamp_precision'] = 'ms'

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


def produce(msg_source,
            connection_url=None,
            publish_limit=constants.DEFAULT_PUBLISH_LIMIT,
            publish_interval=constants.DEFAULT_PUBLISH_INTERVAL,
            verbose=False):
    """Publishes message(s) to MQ server.

    :param msg_source: Source of messages for publishing.
    :type msg_source: Message | function
    :param str connection_url: An MQ server connection URL.
    :param int publish_limit: Maximum number of message publishing events.
    :param int publish_interval: Frequency at which message(s) are published.
    :param bool verbose: Flag indicating whether logging level is verbose or not.

    """
    # Instantiate producer.
    producer = Producer(msg_source,
                        connection_url=connection_url,
                        publish_limit=publish_limit,
                        publish_interval=publish_interval,
                        verbose=verbose)

    # Run.
    try:
        producer.run()
    except KeyboardInterrupt:
        producer.stop()


def consume(exchange,
            queue,
            callback,
            auto_persist=False,
            connection_url=None,
            consume_limit=0,
            context_type=message.Message,
            verbose=False):
    """Consumes message(s) from an MQ server.

    :param str exchange: Name of an exchange to bind to.
    :param str queue: Name of queue to bind to.
    :param func callback: Function to invoke when message has been handled.

    :param bool auto_persist: Flag indicating whether message is to be persisted to db.
    :param str connection_url: An MQ server connection URL.
    :param int consume_limit: Limit upon number of message to be consumed.
    :param class context_type: Type of message processing context object to instantiate.
    :param bool verbose: Flag indicating whether logging level is verbose.

    """
    # Handles message being consumed.
    def msg_handler(ctx):
        """Handles message being consumed."""
        if auto_persist:
            # Abort processing of duplicate messages.
            try:
                ctx.msg = persist(ctx.properties, ctx.content_raw)
            except IntegrityError as err:
                rt.log_mq("WARNING :: duplicate message :: {}".format(ctx.properties.message_id))
            except Exception as err:
                print err
            else:
                callback(ctx)
        else:
            callback(ctx)

    # Instantiate producer.
    consumer = Consumer(exchange,
                        queue,
                        msg_handler,
                        connection_url=connection_url,
                        consume_limit=consume_limit,
                        context_type=context_type,
                        verbose=verbose)

    # Run.
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()


def _get_timestamps(properties):
    """Returns timestamps used during persistence.

    """
    # Set precision.
    if 'timestamp_precision' in properties.headers:
        precision = properties.headers["timestamp_precision"]
    else:
        precision = 'ms'

    # Set raw.
    raw = properties.headers["timestamp"]

    # Set parsed.
    if precision == 'ns':
        parsed = Timestamp.from_ns(raw).as_ms.datetime
    else:
        parsed = Timestamp.from_ms(raw).as_ms.datetime

    return precision, raw, parsed


def persist(properties, payload):
    """Persists message to backend db.

    :param pika.BasicProperties properties: Message AMPQ properties.
    :param str payload: Message payload.

    :returns: Persisted message.
    :rtype: prodiguer.db.Message

    """
    # TODO: persist message mode ?
    # TODO: persist message user-id ?

    # Get timestamp info.
    ts_precision, ts_raw, ts_parsed = _get_timestamps(properties)

    return db.mq_hooks.create_message(
        properties.message_id,
        properties.app_id,
        properties.headers['producer_id'],
        properties.type,
        payload,
        properties.content_encoding,
        properties.content_type,
        ts_parsed,
        ts_precision,
        ts_raw)
