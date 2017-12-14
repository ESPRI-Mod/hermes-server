# -*- coding: utf-8 -*-

"""
.. module:: hermes.mq.utils.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of message queue utilty functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
import uuid

import arrow
import pika
import sqlalchemy

from hermes.db import pgres as db
from hermes.db.pgres.constants import DEFAULT_TZ
from hermes.mq import constants
from hermes.mq import defaults
from hermes.mq import message
from hermes.mq.consumer import Consumer
from hermes.mq.producer import Producer
from hermes.utils import logger
from hermes.utils import validation
from hermes.utils.config import data as config



# Configuration used by the module.
_CONFIG = config.mq


def create_ampq_message_properties(
    user_id,
    producer_id,
    producer_version,
    message_type,
    message_id=None,
    headers=None,
    cluster_id=None,
    content_encoding=defaults.DEFAULT_CONTENT_ENCODING,
    content_type=defaults.DEFAULT_CONTENT_TYPE,
    correlation_id=None,
    delivery_mode = defaults.DEFAULT_DELIVERY_MODE,
    expiration=None,
    priority=defaults.DEFAULT_PRIORITY,
    reply_to=None,
    timestamp=None,
    delay_in_ms=None,
    exchange=None
    ):
    """Factory function to return set of AMQP message properties.

    :param str user_id: ID of AMPQ user account under which messages are being dispatched.
    :param str producer_id: Message producer identifier.
    :param str producer_version: Message producer version identifier.
    :param str message_type: Message type identifier.
    :param uuid message_id: Message unique identifier.
    :param dict headers: Custom message headers.
    :param str cluster_id: ID of MQ cluster.
    :param str content_encoding: Content encoding, e.g. utf-8.
    :param str content_type: Content MIME type, e.g. application/json.
    :param str correlation_id: Application correlation identifier.
    :param int delivery_mode: Message delivery mode (2 = with acknowledgement).
    :param int expiration: Ticks until message will no be considered as active.
    :param int priority: Messaging priority.
    :param str reply_to: Messaging RPC callback.
    :param int timestamp: The message timestamp.
    :param int delay_in_ms: Delay (in milliseconds) before message is routed.
    :param str exchange: Overridden AMPQ exchange to which message will be published.

    :returns pika.BasicProperties: Set of AMPQ message basic properties.

    """
    # Override null inputs.
    if headers is None:
        headers = {}
    if message_id is None:
        message_id = unicode(uuid.uuid4())

    # Validate inputs.
    validation.validate_uid(message_id, "message_id")
    validation.validate_mbr(content_encoding, constants.CONTENT_ENCODINGS, 'content encoding')
    validation.validate_mbr(content_type, constants.CONTENT_TYPES, 'content type')
    if cluster_id:
        raise NotImplementedError()
    if correlation_id:
        validation.validate_uid(correlation_id)
    validation.validate_mbr(delivery_mode, constants.AMPQ_DELIVERY_MODES, 'delivery mode')
    if expiration:
        raise NotImplementedError()
    validation.validate_mbr(message_type, constants.TYPES, 'message type')
    validation.validate_mbr(producer_id, constants.PRODUCERS, 'producer id')
    validation.validate_vrs(producer_version, 'producer version')
    validation.validate_mbr(priority, constants.PRIORITIES, 'priority')
    if reply_to:
        raise NotImplementedError()
    if timestamp:
        validation.validate_int(timestamp, 'timestamp')
    validation.validate_mbr(user_id, constants.USERS, 'user id')
    if exchange:
        validation.validate_mbr(exchange, constants.EXCHANGES, 'AMPQ exchange')

    # Set timestamps.
    # ... if not provided then create one.
    if timestamp is None:
        timestamp = arrow.utcnow()
        headers['timestamp'] = unicode(timestamp.isoformat())
        timestamp = int(repr(timestamp.float_timestamp).replace(".", ""))

    # ... if timestamp not in header then inject.
    if 'timestamp' not in headers:
        headers['timestamp'] = unicode(arrow.utcnow().isoformat())

    # ... if timestamp not in header then inject.
    if 'timestamp_raw' not in headers:
        headers['timestamp_raw'] = headers['timestamp']

    # Set other headers.
    if 'producer_id' not in headers:
        headers['producer_id'] = producer_id
    if 'producer_version' not in headers:
        headers['producer_version'] = producer_version
    if delay_in_ms is not None:
        headers['x-delay'] = delay_in_ms

    # Set exchange.
    if exchange:
        headers['exchange'] = exchange

    # Remove null headers.
    headers = {k: v for k, v in headers.iteritems() if v is not None}

    # Return a pika BasicProperties instance (follows AMPQ protocol).
    return pika.BasicProperties(
        app_id=constants.MESSAGE_TYPE_APPLICATION[message_type],
        cluster_id=cluster_id,
        content_type=content_type,
        content_encoding=content_encoding,
        correlation_id=correlation_id,
        delivery_mode=delivery_mode,
        expiration=expiration,
        headers=headers,
        message_id=message_id,
        priority=priority,
        reply_to=reply_to,
        timestamp=timestamp,
        type=message_type,
        user_id=user_id
        )


def produce(
    msg_source,
    connection_url=None,
    publish_limit=defaults.DEFAULT_PUBLISH_LIMIT,
    publish_interval=defaults.DEFAULT_PUBLISH_INTERVAL,
    verbose=False
    ):
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


def _process_message(ctx, callback):
    """Processes a message being consumed from MQ server.

    """
    with db.session.create():
        # Persist message to dB.
        try:
            ctx.msg = _persist(ctx.properties, ctx.content_raw)

        # Skip duplicate messages.
        except sqlalchemy.exc.IntegrityError:
            msg = "Duplicate message skipped: TYPE={};  UID={}"
            msg = msg.format(ctx.properties.type, ctx.properties.message_id)
            logger.log_mq_warning(msg)
            db.session.rollback()

        # Log persistence errors.
        except Exception as err:
            logger.log_mq_error(err)

        # Invoke message processing callback.
        else:
            callback(ctx)
            if ctx.properties.type in _CONFIG.deletableTypes:
                msg = "TODO :: auto-=delete message : TYPE={};  UID={}"
                msg = msg.format(ctx.properties.type, ctx.properties.message_id)
                logger.log_mq_warning(msg)


def consume(
    exchange,
    queue,
    callback,
    connection_url=None,
    consume_limit=0,
    context_type=message.Message,
    verbose=False
    ):
    """Consumes message(s) from an MQ server.

    :param str exchange: Name of an exchange to bind to.
    :param str queue: Name of queue to bind to.
    :param func callback: Function to invoke when message has been handled.
    :param str connection_url: An MQ server connection URL.
    :param int consume_limit: Limit upon number of message to be consumed.
    :param class context_type: Type of message processing context object to instantiate.
    :param bool verbose: Flag indicating whether logging level is verbose.

    """
    # Instantiate consumer.
    consumer = Consumer(exchange,
                        queue,
                        lambda ctx: _process_message(ctx, callback),
                        connection_url=connection_url,
                        consume_limit=consume_limit,
                        context_type=context_type,
                        verbose=verbose)

    # Run consumer.
    try:
        consumer.run()
    except KeyboardInterrupt:
        consumer.stop()


def _persist(properties, payload):
    """Persists message to backend db.

    :param pika.BasicProperties properties: Message AMPQ properties.
    :param str payload: Message payload.

    :returns: Persisted message.
    :rtype: Message

    """
    def _get_header(key, default=None):
        """Returns a header field.

        """
        if key in properties.headers:
            return properties.headers[key]

        return default

    # Set timestamp info.
    _, timestamp, _, _ = get_timestamps(properties.headers["timestamp"])

    return db.dao_mq.persist_message(
        properties.message_id,
        properties.user_id,
        properties.app_id,
        _get_header('producer_id'),
        _get_header('producer_version'),
        properties.type,
        payload,
        properties.content_encoding,
        properties.content_type,
        _get_header('correlation_id_1'),
        _get_header('correlation_id_2'),
        _get_header('correlation_id_3'),
        timestamp,
        properties.headers["timestamp_raw"],
        _get_header('email_id')
        )


def get_timestamps(raw):
    """Returns timestamp information derived from either a nano or micro second precise ISO timestamp.

    :param str as_raw: A nano or micro second precise timestamp.

    :returns: 4 member tuple of timestamp representations: (raw, utc, integer, text).
    :rtype: tuple

    """
    # Convert to ms precise raw string.
    as_text = "{}.{}+{}".format(raw.split('.')[0], raw.split('.')[1][0:6], raw.split('.')[1][-5:]).replace('++', '+')

    # Convert to UTC.
    as_utc = arrow.get(as_text).to(DEFAULT_TZ)

    # Convert to integer.
    as_int = int("{}{}".format(
            as_utc.timestamp,
            raw.split('.')[1][0:6]
            ))

    return raw, as_utc.datetime, as_int, as_text
