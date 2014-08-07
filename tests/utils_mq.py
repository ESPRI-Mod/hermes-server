# -*- coding: utf-8 -*-

"""
.. module:: utils_mq.py

   :copyright: @2013 Institute Pierre Simon Laplace (http://esdocumentation.org)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Message queue platform utilty test functions.

.. moduleauthor:: Institute Pierre Simon Laplace (ES-DOC) <dev@esdocumentation.org>

"""
import pika

from prodiguer.mq1 import constants

from . import utils as tu



def assert_msg_props(props,
                     app_id=constants.APP_SMON,
                     cluster_id=None,
                     content_encoding=constants.DEFAULT_CONTENT_ENCODING,
                     content_type=constants.DEFAULT_CONTENT_TYPE,
                     correlation_id=None,
                     delivery_mode=constants.AMPQ_DELIVERY_MODE_PERSISTENT,
                     expiration=None,
                     headers={},
                     message_id=None,
                     priority=constants.DEFAULT_PRIORITY,
                     reply_to=None,
                     timestamp=None,
                     type_id=None,
                     user_id=constants.DEFAULT_USER):
    """Asserts the AMPQ basic properties associated with a message."""
    tu.assert_object(props, pika.spec.BasicProperties)
    tu.assert_string(props.app_id, app_id)
    tu.assert_nullable(props.cluster_id, cluster_id, tu.assert_integer)
    tu.assert_string(props.content_encoding, content_encoding)
    tu.assert_string(props.content_type, content_type)
    tu.assert_nullable(props.correlation_id, correlation_id, tu.assert_string)
    tu.assert_integer(props.delivery_mode, delivery_mode)
    tu.assert_nullable(props.expiration, expiration, tu.assert_string)
    tu.assert_dict(props.headers, headers)
    if message_id is None:
        tu.assert_uuid(props.message_id)
    else:
        tu.assert_uuid(props.message_id, message_id)
    tu.assert_integer(props.priority, priority)
    tu.assert_nullable(props.reply_to, reply_to, tu.assert_string)
    tu.assert_obj(props.timestamp, int)
    tu.assert_string(props.type, type_id)
    tu.assert_string(props.user_id, user_id)
