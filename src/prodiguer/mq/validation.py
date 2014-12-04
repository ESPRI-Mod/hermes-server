# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.validation.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Messaging related validation functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
import datetime, uuid

import constants
from timestamp import Timestamp



def validate_app_id(identifer):
    """Validates a messaging application identifier.

    :param str identifer: A Messaging application identifier.

    """
    if identifer not in constants.APPS:
        raise ValueError('Message application is unknown: {0}'.format(identifer))


def validate_producer_id(identifer):
    """Validates a messaging producer identifier.

    :param str identifer: A Messaging producer identifier.

    """
    if identifer not in constants.PRODUCERS:
        raise ValueError('Message producer is unknown: {0}'.format(identifer))


def validate_type_id(identifer):
    """Validates a messaging type identifier.

    :param str identifer: A Messaging type identifier.

    """
    if identifer not in constants.TYPES:
        raise ValueError('Message type is unknown: {0}'.format(identifer))


def validate_uid(identifer):
    """Validates a messaging unique identifier.

    :param str identifer: A Messaging unique identifier.

    """
    try:
        uuid.UUID(identifer)
    except ValueError:
        raise ValueError("Message identifer must be UUID compatible.")


def validate_correlation_id(identifer):
    """Validates a messaging correlation identifier.

    :param str identifer: A Messaging correlation identifier.

    """
    try:
        uuid.UUID(identifer)
    except ValueError:
        raise ValueError("Message correlation identifer must be UUID compatible.")


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
