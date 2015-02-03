# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_mq.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db import types, session



def _validate_create_message(
    uid,
    user_id,
    app_id,
    producer_id,
    type_id,
    content,
    content_encoding,
    content_type,
    correlation_id_1,
    correlation_id_2,
    correlation_id_3,
    timestamp,
    timestamp_precision,
    timestamp_raw,
    mode):
    """Validates create message inputs.

    """
    from prodiguer.mq import validation as msg_validation

    msg_validation.validate_uid(uid)
    msg_validation.validate_user_id(user_id)
    msg_validation.validate_app_id(app_id)
    msg_validation.validate_producer_id(producer_id)
    msg_validation.validate_type_id(type_id)
    if correlation_id_1:
        msg_validation.validate_correlation_id(correlation_id_1)
    if correlation_id_2:
        msg_validation.validate_correlation_id(correlation_id_2)
    if correlation_id_3:
        msg_validation.validate_correlation_id(correlation_id_3)
    msg_validation.validate_content(content, content_encoding, content_type)
    msg_validation.validate_timestamp(timestamp, timestamp_precision, timestamp_raw)
    msg_validation.validate_mode(mode)


def create_message(
    uid,
    user_id,
    app_id,
    producer_id,
    type_id,
    content,
    content_encoding='utf-8',
    content_type='application/json',
    correlation_id_1=None,
    correlation_id_2=None,
    correlation_id_3=None,
    timestamp=None,
    timestamp_precision=None,
    timestamp_raw=None,
    mode=None):
    """Creates a new message record in db.

    :param str uid: Message unique identifer.
    :param str user_id: Message user id, e.g. libl-igcm-user.
    :param str app_id: Message application id, e.g. smon.
    :param str producer_id: Message producer id, e.g. libigcm.
    :param str type_id: Message type id, e.g. 1001000.
    :param str name: Message content.
    :param str content_encoding: Message content encoding, e.g. utf-8.
    :param str content_type: Message content type, e.g. application/json.
    :param str correlation_id_1: Message correlation identifier.
    :param str correlation_id_2: Message correlation identifier.
    :param str correlation_id_3: Message correlation identifier.
    :param datetime.datetime timestamp: Message timestamp.
    :param str timestamp_precision: Message timestamp precision (ns | ms).
    :param str timestamp_raw: Message raw timestamp.
    :param str mode: Message dispatch mode.

    :returns: Newly created message.
    :rtype: types.Message

    """
    # Validate inputs.
    _validate_create_message(
        uid,
        user_id,
        app_id,
        producer_id,
        type_id,
        content,
        content_encoding,
        content_type,
        correlation_id_1,
        correlation_id_2,
        correlation_id_3,
        timestamp,
        timestamp_precision,
        timestamp_raw,
        mode
        )

    # Instantiate instance.
    msg = types.Message()
    msg.app_id = app_id
    msg.content = content
    msg.content_encoding = content_encoding
    msg.content_type = content_type
    msg.correlation_id_1 = correlation_id_1
    msg.correlation_id_2 = correlation_id_2
    msg.correlation_id_3 = correlation_id_3
    msg.mode = mode
    msg.producer_id = producer_id
    if timestamp is not None:
        msg.timestamp = timestamp
    if timestamp_precision is not None:
        msg.timestamp_precision = timestamp_precision
    if timestamp_raw is not None:
        msg.timestamp_raw = timestamp_raw
    msg.type_id = type_id
    msg.uid = uid
    msg.user_id = user_id

    # Push to db.
    session.add(msg)

    return msg


def create_message_email(uid):
    """Creates a new message email record in db.

    """
    instance = types.MessageEmail()
    instance.uid = uid

    # Push to db.
    session.add(instance)

    return instance
