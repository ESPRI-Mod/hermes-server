# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.dao_mq_validation.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations validation.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.mq import validator as msg_validator
from prodiguer.utils import validation



def validate_create_message(
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
    timestamp_precision,
    timestamp_raw,
    email_id
    ):
    """Function input validator: create_message.

    """
    msg_validator.validate_app_id(app_id)
    msg_validator.validate_content(content)
    msg_validator.validate_content_encoding(content_encoding)
    msg_validator.validate_content_type(content_type)
    if correlation_id_1:
        msg_validator.validate_correlation_id(correlation_id_1)
    if correlation_id_2:
        msg_validator.validate_correlation_id(correlation_id_2)
    if correlation_id_3:
        msg_validator.validate_correlation_id(correlation_id_3)
    msg_validator.validate_message_id(uid)
    msg_validator.validate_producer_id(producer_id)
    msg_validator.validate_producer_version(producer_version)
    msg_validator.validate_timestamp_info(timestamp, timestamp_precision, timestamp_raw)
    msg_validator.validate_type(type_id)
    msg_validator.validate_user_id(user_id)
    if email_id:
        msg_validator.validate_email_id(email_id)



def validate_delete_message(uid):
    """Function input validator: delete_message.

    """
    msg_validator.validate_message_id(uid)


def validate_create_message_email(email_id):
    """Function input validator: create_message_email.

    """
    msg_validator.validate_email_id(email_id)


def validate_is_duplicate(uid):
    """Function input validator: is_duplicate.

    """
    msg_validator.validate_message_id(uid)


def validate_retrieve_messages(uid=None):
    """Function input validator: retrieve_messages.

    """
    if uid is not None:
        validation.validate_uid(uid, "Simulation uid")


def validate_has_messages(uid):
    """Function input validator: has_messages.

    """
    validation.validate_uid(uid, "Simulation uid")


def validate_retrieve_message_email(email_id):
    """Function input validator: retrieve_message_email.

    """
    msg_validator.validate_email_id(email_id)


def validate_retrieve_message_emails(arrival_date):
    """Function input validator: retrieve_message_emails.

    """
    pass


def validate_persist_message_email_stats(
    email_id,
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
    outgoing_7100=0
    ):
    """Function input validator: update_message_email_stats.

    """
    msg_validator.validate_email_id(email_id)
    if arrival_date:
        validation.validate_date(arrival_date, 'Email arrival date')
    if dispatch_date:
        validation.validate_date(dispatch_date, 'Email dispatch date')
    validation.validate_int(incoming, "incoming")
    validation.validate_int(errors_decoding_base64, "errors_decoding_base64")
    validation.validate_int(errors_decoding_json, "errors_decoding_json")
    validation.validate_int(errors_encoding_ampq, "errors_encoding_ampq")
    validation.validate_int(excluded, "excluded")
    validation.validate_int(outgoing, "outgoing")
    validation.validate_int(outgoing_0000, "outgoing_0000")
    validation.validate_int(outgoing_0100, "outgoing_0100")
    validation.validate_int(outgoing_1000, "outgoing_1000")
    validation.validate_int(outgoing_1001, "outgoing_1001")
    validation.validate_int(outgoing_1100, "outgoing_1100")
    validation.validate_int(outgoing_1900, "outgoing_1900")
    validation.validate_int(outgoing_1999, "outgoing_1999")
    validation.validate_int(outgoing_2000, "outgoing_2000")
    validation.validate_int(outgoing_2100, "outgoing_2100")
    validation.validate_int(outgoing_2900, "outgoing_2900")
    validation.validate_int(outgoing_2999, "outgoing_2999")
    validation.validate_int(outgoing_7000, "outgoing_7000")
    validation.validate_int(outgoing_7010, "outgoing_7010")
    validation.validate_int(outgoing_7011, "outgoing_7011")
    validation.validate_int(outgoing_7100, "outgoing_7100")
