# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.validation.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Messaging related validation functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
import datetime, uuid

import pika

from prodiguer.mq import constants, timestamp as Timestamp



def validate_ampq_basic_properties(props):
    """Validates AMPQ basic properties associated with message being processed.

    """
    # Validate props are AMPQ BasicProperties.
    if not isinstance(props, pika.BasicProperties):
        raise ValueError("AMPQ message basic properties is invalid.")

    # Validate headers.
    if 'producer_id' not in props.headers:
        msg = "[producer_id] is a required header field."
        raise ValueError(msg)
    validate_producer_id(props.headers['producer_id'])

    # Validate application id.
    validate_app_id(props.app_id)
    validate_cluster_id(props.cluster_id)
    validate_content_encoding(props.content_encoding)
    validate_content_type(props.content_type)
    if props.correlation_id:
        validate_correlation_id(props.correlation_id)
    validate_delivery_mode(props.delivery_mode)
    validate_message_id(props.message_id)
    validate_timestamp(props.timestamp)
    validate_type(props.type)
    validate_user_id(props.user_id)


def validate_app_id(identifier):
    """Validates a messaging application identifier.

    :param str identifier: A Messaging application identifier.

    """
    if identifier not in constants.APPS:
        raise ValueError('Message application is unknown: {}'.format(identifier))


def validate_cluster_id(identifier):
    """Validates a messaging cluster identifier.

    :param str identifier: A Messaging cluster identifier.

    """
    # Not using clusters at this point in time.
    pass


def validate_message_id(identifier):
    """Validates a message identifier.

    :param str identifier: A message type.

    """
    try:
        uuid.UUID(identifier)
    except ValueError:
        raise ValueError("Message identifier must be UUID compatible.")


def validate_producer_id(identifier):
    """Validates a messaging producer identifier.

    :param str identifier: A Messaging producer identifier.

    """
    if identifier not in constants.PRODUCERS:
        raise ValueError('Message producer is unknown: {}'.format(identifier))


def validate_type(identifier):
    """Validates a messaging type identifier.

    :param str identifier: A Messaging type identifier.

    """
    if identifier not in constants.TYPES:
        raise ValueError('Message type is unknown: {}'.format(identifier))


def validate_correlation_id(identifier):
    """Validates a messaging correlation identifier.

    :param str identifier: A Messaging correlation identifier.

    """
    try:
        uuid.UUID(identifier)
    except ValueError:
        raise ValueError("Message correlation identifier must be UUID compatible: {}".format(identifier))


def validate_content(content):
    """Validates message content.

    :param str content: Message content.

    """
    if content is None or not len(content):
        raise TypeError('Message content is empty.')


def validate_content_encoding(content_encoding):
    """Validates message content encoding.

    :param str content_encoding: Message content encoding.

    """
    if content_encoding not in constants.CONTENT_ENCODINGS:
        raise TypeError('Message content encoding is unknown.')


def validate_content_type(content_type):
    """Validates message content type.

    :param str content_type: Message content type.

    """
    if content_type not in constants.CONTENT_TYPES:
        raise TypeError('Message content type is unknown.')


def validate_timestamp(timestamp):
    """Validates message timestamp information.

    :param datetime.datetime timestamp: Message timestamp (micro-second precise).

    """
    pass


def validate_timestamp_info(timestamp, precision, raw):
    """Validates message timestamp information.

    :param datetime.datetime timestamp: Message timestamp (ms precise).
    :param str precision: Message timestamp precision.
    :param str raw: Message raw timestamp.

    """
    if not isinstance(timestamp, datetime.datetime):
        raise ValueError("Timestamp must be a valid datetime.")
    if precision not in constants.TIMESTAMP_PRECISIONS:
        raise TypeError('Timestamp precision is unknown.')
    if raw:
        try:
            Timestamp.create(precision, raw)
        except (ValueError, IndexError):
            raise TypeError('Raw timestamp is unparseable.')


def validate_delivery_mode(mode):
    """Validates message delivery mode.

    :param str mode: A messaging delivery mode.

    """
    try:
        int(mode)
    except ValueError:
        raise ValueError("Message delivery mode must be an integer: {}".format(mode))
    if int(mode) not in constants.AMPQ_DELIVERY_MODES:
        raise ValueError('Message delivery mode is unknown: {}'.format(mode))


def validate_user_id(identifier):
    """Validates user id that dispatched message.

    :param str identifier: Message user id, e.g. libl-igcm-user.

    """
    if identifier not in constants.USERS:
        raise ValueError('Message user id is unknown: {}'.format(identifier))
