# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_mq_validator.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations validator.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.mq import validator as msg_validator



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
    msg_validator.validate_timestamp_info(timestamp, timestamp_precision, timestamp_raw)
    msg_validator.validate_type(type_id)
    msg_validator.validate_user_id(user_id)


def validate_create_message_email(email_id):
    """Function input validator: create_message_email.

    """
    msg_validator.validate_email_id(uid)


def validate_is_duplicate(uid):
    """Function input validator: is_duplicate.

    """
    msg_validator.validate_message_id(uid)

