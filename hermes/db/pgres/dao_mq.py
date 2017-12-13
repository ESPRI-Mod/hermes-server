# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.dao_mq.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime as dt

from hermes.db.pgres import session
from hermes.db.pgres import types
from hermes.db.pgres import validator_dao_mq as validator
from hermes.db.pgres.convertor import as_datetime_string
from hermes.utils import decorators



@decorators.validate(validator.validate_persist_message)
def persist_message(
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
        instance.timestamp = timestamp.datetime
    if timestamp_raw is not None:
        instance.timestamp_raw = unicode(timestamp_raw)

    return session.insert(instance)


@decorators.validate(validator.validate_has_messages)
def has_messages(uid):
    """Retrieves boolean indicating whether a simulation has at least one messages in the db.

    :param str uid: UID of simulation.

    :returns: True if simulation has >= 1 message, false otherwise.
    :rtype: bool

    """
    qry = session.query(types.Message)
    qry = qry.filter(types.Message.correlation_id_1 == unicode(uid))
    # qry = qry.filter(types.Message.type_id != u'7000')

    return qry.first() is not None


@decorators.validate(validator.validate_retrieve_messages)
def retrieve_messages(uid=None, exclude_excessive=True):
    """Retrieves message details from db.

    :param str uid: Correlation UID.
    :param bool exclude_excessive: Flag indicating whether excessive message types are to be excluded from results.

    :returns: List of message associated with a simulation.
    :rtype: list

    """
    m = types.Message

    qry = session.raw_query(
        m.content,
        m.email_id,
        m.correlation_id_2,
        as_datetime_string(m.row_create_date),
        m.producer_version,
        as_datetime_string(m.timestamp),
        m.type_id,
        m.uid
        )
    if uid is not None:
        qry = qry.filter(m.correlation_id_1 == uid)
    if exclude_excessive:
        for msg_type  in {u'7000', u'1900', u'2900'}:
            qry = qry.filter(m.type_id != msg_type)
    qry = qry.order_by(m.timestamp)

    return qry.all()


def _retrieve_message_email(email_id):
    """Retrieves a message email record from db.

    :param str email_id: Email identifier (assigned by SMTP server).

    :returns: Message email instance.
    :rtype: types.MessageEmail

    """
    qry = session.query(types.MessageEmail)
    qry = qry.filter(types.MessageEmail.email_id == int(email_id))

    return qry.first()


@decorators.validate(validator.validate_retrieve_message_emails)
def retrieve_message_emails(arrival_date):
    """Retrieves a collection of message email records from db.

    :param datetime arrival_date: Date from which emails will be retrieved.

    :returns: Sequence of message email instances.
    :rtype: list

    """
    qry = session.query(types.MessageEmail)
    qry = qry.filter(types.MessageEmail.row_create_date >= arrival_date)

    return qry.all()


@decorators.validate(validator.validate_persist_message_email)
def persist_message_email(email_id):
    """Creates a new message email record in db.

    :param str email_id: Email identifier (assigned by SMTP server).

    :returns: Message email instance.
    :rtype: types.MessageEmail

    """
    instance = types.MessageEmail()
    instance.email_id = email_id

    return session.insert(instance)


def _update_message_email(email_id, arrival_date, dispatch_date):
    """Updates a message email with simple statistical information.

    :param str email_id: Email identifier (assigned by SMTP server).
    :param datetime.datetime arrival_date: Email arrival date.
    :param datetime.datetime dispatch_date: Email dispatch date.

    """
    # Escape if email body did not contain relevant date fields.
    if arrival_date is None and dispatch_date is None:
        return

    # Escape if email db entry is not yet written.
    email = _retrieve_message_email(email_id)
    if email is None:
        return

    email.arrival_date = arrival_date
    email.dispatch_date = dispatch_date
    if arrival_date is not None and dispatch_date is not None:
        email.arrival_latency = (arrival_date - dispatch_date).total_seconds()

    session.update(email)


@decorators.validate(validator.validate_persist_message_email_stats)
def persist_message_email_stats(
    email_server_id,
    email_id,
    email_rejected,
    arrival_date=None,
    dispatch_date=None,
    incoming=0,
    errors_decoding_base64=0,
    errors_decoding_json=0,
    errors_encoding_ampq=0,
    excluded=0,
    outgoing=0,
    outgoing_0000=0,
    outgoing_0100=0,
    outgoing_1000=0,
    outgoing_1001=0,
    outgoing_1100=0,
    outgoing_1900=0,
    outgoing_1999=0,
    outgoing_2000=0,
    outgoing_2100=0,
    outgoing_2900=0,
    outgoing_2999=0,
    outgoing_7000=0,
    outgoing_7010=0,
    outgoing_7011=0,
    outgoing_7100=0,
    outgoing_8888=0
    ):
    """Updates a message email with statistical information.

    :param str email_id: Email identifier (assigned by SMTP server).
    :param bool email_rejected: Flag indicating that email was rejected.
    :param datetime arrival_date: Date of email arrival.
    :param datetime dispatch_date: Date of email dispatch.
    :param int incoming: Count of incoming messages.
    :param int errors_decoding_base64: Count of base64 decoding errors.
    :param int errors_decoding_json: Count of json decoding errors.
    :param int errors_encoding_ampq: Count of ampq encoding errors.
    :param int excluded: Count of excluded messages.
    :param int outgoing: Count of messages dispatched to RabbitMQ server.
    :param int outgoing_0000: Count of messages (type=0000) dispatched to RabbitMQ server.
    :param int outgoing_0100: Count of messages (type=0100) dispatched to RabbitMQ server.
    :param int outgoing_1000: Count of messages (type=1000) dispatched to RabbitMQ server.
    :param int outgoing_1001: Count of messages (type=1001) dispatched to RabbitMQ server.
    :param int outgoing_1100: Count of messages (type=1100) dispatched to RabbitMQ server.
    :param int outgoing_1900: Count of messages (type=1900) dispatched to RabbitMQ server.
    :param int outgoing_1999: Count of messages (type=1999) dispatched to RabbitMQ server.
    :param int outgoing_2000: Count of messages (type=2000) dispatched to RabbitMQ server.
    :param int outgoing_2100: Count of messages (type=2100) dispatched to RabbitMQ server.
    :param int outgoing_2900: Count of messages (type=2900) dispatched to RabbitMQ server.
    :param int outgoing_2999: Count of messages (type=2999) dispatched to RabbitMQ server.
    :param int outgoing_7000: Count of messages (type=7000) dispatched to RabbitMQ server.
    :param int outgoing_7010: Count of messages (type=7010) dispatched to RabbitMQ server.
    :param int outgoing_7011: Count of messages (type=7011) dispatched to RabbitMQ server.
    :param int outgoing_7100: Count of messages (type=7100) dispatched to RabbitMQ server.
    :param int outgoing_8888: Count of messages (type=8888) dispatched to RabbitMQ server.

    """
    _update_message_email(email_id, arrival_date, dispatch_date)

    instance = types.MessageEmailStats()
    instance.email_server_id = email_server_id
    instance.email_id = email_id
    instance.email_rejected = email_rejected
    instance.arrival_date = arrival_date
    instance.dispatch_date = dispatch_date
    if arrival_date is not None and dispatch_date is not None:
        instance.arrival_latency = (arrival_date - dispatch_date).total_seconds()
    instance.incoming = incoming
    instance.errors_decoding_base64 = errors_decoding_base64
    instance.errors_decoding_json = errors_decoding_json
    instance.errors_encoding_ampq = errors_encoding_ampq
    instance.excluded = excluded
    instance.outgoing = outgoing
    instance.outgoing_0000 = outgoing_0000
    instance.outgoing_0100 = outgoing_0100
    instance.outgoing_1000 = outgoing_1000
    instance.outgoing_1001 = outgoing_1001
    instance.outgoing_1100 = outgoing_1100
    instance.outgoing_1900 = outgoing_1900
    instance.outgoing_1999 = outgoing_1999
    instance.outgoing_2000 = outgoing_2000
    instance.outgoing_2100 = outgoing_2100
    instance.outgoing_2900 = outgoing_2900
    instance.outgoing_2999 = outgoing_2999
    instance.outgoing_7000 = outgoing_7000
    instance.outgoing_7010 = outgoing_7010
    instance.outgoing_7011 = outgoing_7011
    instance.outgoing_7100 = outgoing_7100
    instance.outgoing_8888 = outgoing_8888

    return session.insert(instance)


def retrieve_mail_identifiers_by_interval(interval_start, interval_end):
    """Retrieves set of mail identifiers filtered by creation interval.

    :param datetime interval_start: Interval start date.
    :param datetime interval_end: Interval end date.

    :returns: Job details.
    :rtype: list

    """
    mes = types.MessageEmailStats
    qry = session.raw_query(
        mes.email_id
        )
    qry = qry.filter(mes.arrival_date >= interval_start)
    qry = qry.filter(mes.arrival_date < interval_end)

    return set([m[0] for m in qry.all()])


def get_earliest_mail():
    """Retrieves earliest job in database.

    """
    m = types.MessageEmailStats

    qry = session.query(m)
    qry = qry.filter(m.arrival_date != None)
    qry = qry.order_by(m.arrival_date)

    return qry.first()


def retrieve_mail_simulation_identifiers():
    """Retrieves set of simulation identifiers for each email recieved.

    """
    m = types.Message

    qry = session.raw_query(
        m.email_id,
        m.correlation_id_1,
        )
    qry = qry.filter(m.correlation_id_1 != None)
    qry = qry.filter(m.email_id != None)
    qry = qry.distinct(m.email_id)

    return qry.all()
