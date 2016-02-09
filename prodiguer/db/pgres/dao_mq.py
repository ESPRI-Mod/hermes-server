# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_mq.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres import session
from prodiguer.db.pgres import types
from prodiguer.db.pgres import validator_dao_mq as validator
from prodiguer.db.pgres.convertor import as_datetime_string
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


@decorators.validate(validator.validate_has_messages)
def has_messages(uid):
    """Retrieves boolean indicating whether a simulation has at least one messages in the db.

    :param str uid: UID of simulation.

    :returns: True if simulation has >= 1 message, false otherwise.
    :rtype: bool

    """
    qry = session.query(types.Message)
    qry = qry.filter(types.Message.correlation_id_1 == unicode(uid))
    qry = qry.filter(types.Message.type_id != u'7000')

    return qry.first() is not None


@decorators.validate(validator.validate_retrieve_messages)
def retrieve_messages(uid=None):
    """Retrieves message details from db.

    :param str uid: Correlation UID.

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
        qry = qry.filter(m.type_id != u'7000')
    qry = qry.order_by(m.timestamp)

    return qry.all()


@decorators.validate(validator.validate_retrieve_message_email)
def retrieve_message_email(email_id):
    """Retrieves a message email record from db.

    :param str email_id: Email identifier (assigned by SMTP server).

    :returns: Message email instance.
    :rtype: types.MessageEmail

    """
    qry = session.query(types.MessageEmail)
    qry = qry.filter(types.MessageEmail.uid == int(email_id))

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


@decorators.validate(validator.validate_create_message_email)
def create_message_email(email_id):
    """Creates a new message email record in db.

    :param str email_id: Email identifier (assigned by SMTP server).

    :returns: Message email instance.
    :rtype: types.MessageEmail

    """
    instance = types.MessageEmail()
    instance.uid = email_id

    return session.add(instance)


@decorators.validate(validator.validate_is_duplicate)
def is_duplicate(email_id):
    """Returns true if a message with the same uid already exists in the db.

    :param str email_id: Email identifier (assigned by SMTP server).

    :returns: Flag indicating whether email is a duplicate.
    :rtype: bool

    """
    return retrieve_message_email(email_id) is not None


@decorators.validate(validator.validate_update_message_email)
def update_message_email(email_id, arrival_date, dispatch_date):
    """Updates a message email with statistical information.

    :param str email_id: Email identifier (assigned by SMTP server).
    :param datetime.datetime arrival_date: Email arrival date.
    :param datetime.datetime dispatch_date: Email dispatch date.

    """
    # Escape if email body did not contain relevant date fields.
    if arrival_date is None and dispatch_date is None:
        return

    # Escape if email db entry is not yet written.
    email = retrieve_message_email(email_id)
    if email is None:
        return

    email.arrival_date = arrival_date
    email.dispatch_date = dispatch_date
    if arrival_date is not None and dispatch_date is not None:
        email.dispatch_latency = (arrival_date - dispatch_date).total_seconds()

    session.update(email)
