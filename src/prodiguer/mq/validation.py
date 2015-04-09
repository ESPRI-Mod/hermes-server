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
    # print "TODO - refoactor prodiguer.mq.message.py_validate_ampq_basic_properties"
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


def validate_app_id(identifier):
    """Validates a messaging application identifier.

    :param str identifier: A Messaging application identifier.

    """
    if identifier not in constants.APPS:
        raise ValueError('Message application is unknown: {0}'.format(identifier))


def validate_producer_id(identifier):
    """Validates a messaging producer identifier.

    :param str identifier: A Messaging producer identifier.

    """
    if identifier not in constants.PRODUCERS:
        raise ValueError('Message producer is unknown: {0}'.format(identifier))


def validate_type_id(identifier):
    """Validates a messaging type identifier.

    :param str identifier: A Messaging type identifier.

    """
    if identifier not in constants.TYPES:
        raise ValueError('Message type is unknown: {0}'.format(identifier))


def validate_uid(identifier):
    """Validates a messaging unique identifier.

    :param str identifier: A Messaging unique identifier.

    """
    try:
        uuid.UUID(identifier)
    except ValueError:
        raise ValueError("Message identifier must be UUID compatible.")


def validate_correlation_id(identifier):
    """Validates a messaging correlation identifier.

    :param str identifier: A Messaging correlation identifier.

    """
    try:
        uuid.UUID(identifier)
    except ValueError:
        raise ValueError("Message correlation identifier must be UUID compatible: {}".format(identifier))


def validate_content(content, content_encoding, content_type):
    """Validates message content.

    :param str content: Message content.
    :param str content_encoding: Message content encoding.
    :param str content_type: Message content type.

    """
    if content is None or not len(content):
        raise TypeError('Message content is empty.')
    if content_encoding not in constants.CONTENT_ENCODINGS:
        raise TypeError('Message content encoding is unknown.')
    if content_type not in constants.CONTENT_TYPES:
        raise TypeError('Message content type is unknown.')


def validate_timestamp(timestamp, precision, raw):
    """Validates message timestamp.

    :param datetime.datetime timestamp: Message timestamp (micro-second precise).
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


def validate_mode(mode):
    """Validates message mode.

    :param str mode: A messaging dispatch mode.

    """
    if mode not in constants.MODES:
        raise ValueError('Message mode is unknown: {0}'.format(mode))


def validate_user_id(identifier):
    """Validates message mode.

    :param str identifier: Message user id, e.g. libl-igcm-user.

    """
    if identifier not in constants.USERS:
        raise ValueError('Message user id is unknown: {0}'.format(identifier))
