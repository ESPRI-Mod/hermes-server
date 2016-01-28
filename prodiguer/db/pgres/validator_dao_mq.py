# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_mq_validation.py
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


def validate_create_message_email(email_id):
    """Function input validator: create_message_email.

    """
    msg_validator.validate_email_id(email_id)


def validate_is_duplicate(uid):
    """Function input validator: is_duplicate.

    """
    msg_validator.validate_message_id(uid)


def validate_retrieve_message_email(email_id):
    """Function input validator: retrieve_message_email.

    """
    msg_validator.validate_email_id(email_id)


def validate_retrieve_message_emails(arrival_date):
    """Function input validator: retrieve_message_emails.

    """
    pass


def validate_update_message_email(email_id, arrival_date, dispatch_date):
    """Function input validator: update_message_email.

    """
    msg_validator.validate_email_id(email_id)
    if arrival_date:
        validation.validate_date(arrival_date, 'Email arrival date')
    if dispatch_date:
        validation.validate_date(dispatch_date, 'Email dispatch date')

