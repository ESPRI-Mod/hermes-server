# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_mq.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres import session
from prodiguer.db.pgres import types
from prodiguer.mq import validation as msg_validation
from prodiguer.utils import decorators



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
    timestamp_raw
    ):
    """Validates create message inputs.

    """

    msg_validation.validate_app_id(app_id)
    msg_validation.validate_content(content)
    msg_validation.validate_content_encoding(content_encoding)
    msg_validation.validate_content_type(content_type)
    if correlation_id_1:
        msg_validation.validate_correlation_id(correlation_id_1)
    if correlation_id_2:
        msg_validation.validate_correlation_id(correlation_id_2)
    if correlation_id_3:
        msg_validation.validate_correlation_id(correlation_id_3)
    msg_validation.validate_message_id(uid)
    msg_validation.validate_producer_id(producer_id)
    msg_validation.validate_timestamp_info(timestamp, timestamp_precision, timestamp_raw)
    msg_validation.validate_type(type_id)
    msg_validation.validate_user_id(user_id)


def _validate_create_message_email(email_id):
    """Validates create message inputs.

    """
    pass


@decorators.validate(_validate_create_message)
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
    timestamp_raw=None
    ):
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

    :returns: Newly created message.
    :rtype: types.Message

    """
    instance = types.Message()
    instance.app_id = unicode(app_id)
    instance.content = content
    instance.content_encoding = unicode(content_encoding)
    instance.content_type = unicode(content_type)
    if correlation_id_1:
        instance.correlation_id_1 = unicode(correlation_id_1)
    if correlation_id_2:
        instance.correlation_id_2 = unicode(correlation_id_2)
    if correlation_id_3:
        instance.correlation_id_3 = unicode(correlation_id_3)
    instance.producer_id = unicode(producer_id)
    if timestamp is not None:
        instance.timestamp = timestamp
    if timestamp_precision is not None:
        instance.timestamp_precision = unicode(timestamp_precision)
    if timestamp_raw is not None:
        instance.timestamp_raw = unicode(timestamp_raw)
    instance.type_id = unicode(type_id)
    instance.uid = unicode(uid)
    instance.user_id = unicode(user_id)

    # Push to db.
    session.add(instance)

    return instance


@decorators.validate(_validate_create_message_email)
def create_message_email(email_id):
    """Creates a new message email record in db.

    """
    instance = types.MessageEmail()
    instance.uid = email_id

    # Push to db.
    session.add(instance)

    return instance
