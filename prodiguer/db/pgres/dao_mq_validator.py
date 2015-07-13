# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_mq_validator.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access validation operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.mq import validation as msg_validation



def validate_create_message(
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
    """Function input validator: create_message.

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


def validate_create_message_email(email_id):
    """Function input validator: create_message_email.

    """
    pass


def validate_is_duplicate(uid):
    """Function input validator: is_duplicate.

    """
    pass


def validate_reset_emails():
    """Function input validator: reset_emails.

    """
    pass

