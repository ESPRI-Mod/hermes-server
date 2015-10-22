# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_mq.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres import dao
from prodiguer.db.pgres import session
from prodiguer.db.pgres import types
from prodiguer.db.pgres import validator_dao_mq as validator
from prodiguer.utils import decorators



@decorators.validate(validator.validate_create_message)
def create_message(
    uid,
    user_id,
    app_id,
    producer_id,
    producer_version,
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
    email_id=None
    ):
    """Creates a new message record in db.

    :param str uid: Message unique identifer.
    :param str user_id: Message user id, e.g. libl-igcm-user.
    :param str app_id: Message application id, e.g. smon.
    :param str producer_id: Message producer id, e.g. libigcm.
    :param str producer_version: Message producer version identifier.
    :param str type_id: Message type id, e.g. 0000.
    :param str name: Message content.
    :param str content_encoding: Message content encoding, e.g. utf-8.
    :param str content_type: Message content type, e.g. application/json.
    :param str correlation_id_1: Message correlation identifier.
    :param str correlation_id_2: Message correlation identifier.
    :param str correlation_id_3: Message correlation identifier.
    :param datetime.datetime timestamp: Message timestamp.
    :param str timestamp_precision: Message timestamp precision (ns | ms).
    :param str timestamp_raw: Message raw timestamp.
    :param int email_id: ID of email in which the message was dispatched.

    :returns: Newly created message.
    :rtype: types.Message

    """
    instance = types.Message()
    instance.app_id = unicode(app_id)
    instance.content = content
    instance.content_encoding = unicode(content_encoding)
    instance.content_type = unicode(content_type)
    instance.producer_id = unicode(producer_id)
    instance.producer_version = unicode(producer_version)
    instance.type_id = unicode(type_id)
    instance.uid = unicode(uid)
    instance.user_id = unicode(user_id)
    if correlation_id_1:
        instance.correlation_id_1 = unicode(correlation_id_1)
    if correlation_id_2:
        instance.correlation_id_2 = unicode(correlation_id_2)
    if correlation_id_3:
        instance.correlation_id_3 = unicode(correlation_id_3)
    if email_id:
        instance.email_id = int(email_id)
    if timestamp is not None:
        instance.timestamp = timestamp
    if timestamp_precision is not None:
        instance.timestamp_precision = unicode(timestamp_precision)
    if timestamp_raw is not None:
        instance.timestamp_raw = unicode(timestamp_raw)

    return session.add(instance)


@decorators.validate(validator.validate_create_message_email)
def create_message_email(email_id):
    """Creates a new message email record in db.

    """
    instance = types.MessageEmail()
    instance.uid = email_id

    return session.add(instance)


@decorators.validate(validator.validate_is_duplicate)
def is_duplicate(uid):
    """Returns true if a message with the same uid already exists in the db.

    """
    qfilter = types.Message.uid == unicode(uid)

    return dao.get_by_facet(types.Message, qfilter=qfilter) is not None
