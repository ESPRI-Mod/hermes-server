# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.dao_mq_validation.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations validation.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.mq import constants
from prodiguer.utils.validation import validate_bool
from prodiguer.utils.validation import validate_date
from prodiguer.utils.validation import validate_int
from prodiguer.utils.validation import validate_mbr
from prodiguer.utils.validation import validate_str
from prodiguer.utils.validation import validate_uid
from prodiguer.utils.validation import validate_vrs



def validate_persist_message(
    uid,
    user_id,
    app_id,
    producer_id,
    producer_version,
    type_id,
    content,
    content_encoding,
    content_type,
    correlation_id_1,
    correlation_id_2,
    correlation_id_3,
    timestamp,
    timestamp_raw,
    email_id
    ):
    """Function input validator: persist_message.

    """
    validate_mbr(app_id, constants.APPS, 'message application')
    validate_str(content, "content")
    validate_mbr(content_encoding, constants.CONTENT_ENCODINGS, 'content encoding')
    validate_mbr(content_type, constants.CONTENT_TYPES, 'content type')
    if correlation_id_1:
        validate_uid(correlation_id_1, "correlation_id_1_555")
    if correlation_id_2:
        validate_uid(correlation_id_2, "correlation_id_2_666")
    if correlation_id_3:
        validate_uid(correlation_id_3, "correlation_id_3")
    if email_id:
        validate_int(email_id, "email_id")
    validate_mbr(type_id, constants.TYPES, 'message type')
    validate_mbr(user_id, constants.USERS, 'user type')
    validate_mbr(producer_id, constants.PRODUCERS, 'producer id')
    validate_vrs(producer_version, 'producer version')
    validate_date(timestamp, "timestamp")
    validate_str(timestamp_raw, "timestamp_raw")
    validate_uid(uid, "message_id")


def validate_persist_message_email(email_id):
    """Function input validator: persist_message_email.

    """
    validate_int(email_id, "email_id")


def validate_retrieve_messages(uid=None):
    """Function input validator: retrieve_messages.

    """
    if uid is not None:
        validate_uid(uid, "Simulation uid")


def validate_has_messages(uid):
    """Function input validator: has_messages.

    """
    validate_uid(uid, "Simulation uid")


def validate_retrieve_message_emails(arrival_date):
    """Function input validator: retrieve_message_emails.

    """
    pass


def validate_persist_message_email_stats(
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
    """Function input validator: update_message_email_stats.

    """
    validate_int(email_id, "email_id")
    validate_bool(email_rejected, "email rejected flag")
    if arrival_date:
        validate_date(arrival_date, 'Email arrival date')
    if dispatch_date:
        validate_date(dispatch_date, 'Email dispatch date')
    validate_int(incoming, "incoming")
    validate_int(errors_decoding_base64, "errors_decoding_base64")
    validate_int(errors_decoding_json, "errors_decoding_json")
    validate_int(errors_encoding_ampq, "errors_encoding_ampq")
    validate_int(excluded, "excluded")
    validate_int(outgoing, "outgoing")
    validate_int(outgoing_0000, "outgoing_0000")
    validate_int(outgoing_0100, "outgoing_0100")
    validate_int(outgoing_1000, "outgoing_1000")
    validate_int(outgoing_1001, "outgoing_1001")
    validate_int(outgoing_1100, "outgoing_1100")
    validate_int(outgoing_1900, "outgoing_1900")
    validate_int(outgoing_1999, "outgoing_1999")
    validate_int(outgoing_2000, "outgoing_2000")
    validate_int(outgoing_2100, "outgoing_2100")
    validate_int(outgoing_2900, "outgoing_2900")
    validate_int(outgoing_2999, "outgoing_2999")
    validate_int(outgoing_7000, "outgoing_7000")
    validate_int(outgoing_7010, "outgoing_7010")
    validate_int(outgoing_7011, "outgoing_7011")
    validate_int(outgoing_7100, "outgoing_7100")
    validate_int(outgoing_8888, "outgoing_8888")
