# -*- coding: utf-8 -*-

"""
.. module:: hermes.mq.message.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: A message wrapper.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
import base64
import json

import pika

from prodiguer.mq import constants
from prodiguer.utils import config
from prodiguer.utils import convert
from prodiguer.utils.validation import validate_int
from prodiguer.utils.validation import validate_mbr
from prodiguer.utils.validation import validate_uid
from prodiguer.utils.validation import validate_vrs



class Message(object):
    """Wraps a message either being consumed or produced."""
    def __init__(self, props, content, decode=False, validate_props=True):
        """Constructor.

        :param pika.BasicProperties props: Set of AMPQ properties associated with the message.
        :param object content: Message content.
        :param bool decode: Flag indicating whether message payload is to be decoded.
        :param bool validate_props: Flag indicating whether AMPQ properties need to be validated.

        """
        if validate_props:
            _validate_basic_properties(props)

        self.abort = False
        self.content = content
        self.content_raw = content
        self.content_type = props.content_type
        self.exchange = props.headers.get('exchange', constants.MESSAGE_TYPE_EXCHANGE[props.type])
        self.msg = None
        self.props = self.properties = props
        self.routing_key = "{}.{}.{}.{}.{}".format(
            config.deploymentMode,
            props.user_id,
            props.headers['producer_id'],
            props.app_id,
            props.type
            ).lower()
        if decode:
            self.decode()


    def decode(self):
        """Decodes message content."""
        def _json(content):
            try:
                return json.loads(content)
            except ValueError:
                raise Exception("json encoding error:\n{0}".format(content))

        def _base64(content):
            try:
                return base64.b64decode(content)
            except TypeError:
                raise Exception("Base64 decoding error:\n{0}".format(content))

        if self.content_type in (None, constants.CONTENT_TYPE_JSON):
            self.content = _json(self.content_raw)
        elif self.content_type == constants.CONTENT_TYPE_BASE64:
            self.content = _base64(self.content_raw)
        elif self.content_type == constants.CONTENT_TYPE_BASE64_JSON:
            self.content = _json(_base64(self.content_raw))


    def encode(self):
        """Encodes message content."""
        if isinstance(self.content, dict):
            self.content = convert.dict_to_json(self.content)


    def get_field(self, field_name, default_val=None):
        """Returns value of a message content field.

        """
        val = self.content.get(field_name, default_val)
        if val and val == "null":
            return None
        return val


def _validate_basic_properties(props):
    """Validates AMPQ basic properties associated with message being processed.

    """
    # Validate props are AMPQ BasicProperties.
    if not isinstance(props, pika.BasicProperties):
        raise ValueError("AMPQ message basic properties is invalid.")

    # Validate unsupported properties.
    if props.cluster_id:
        raise ValueError("Unsupported AMPQ basic property: cluster_id")
    if props.expiration:
        raise ValueError("Unsupported AMPQ basic property: expiration")

    # Validate required properties.
    validate_mbr(props.app_id, constants.APPS, 'message application')
    if props.correlation_id:
        validate_uid(props.correlation_id, 'correlation_id')
    validate_mbr(props.content_encoding, constants.CONTENT_ENCODINGS, 'content encoding')
    validate_mbr(props.content_type, constants.CONTENT_TYPES, 'content type')
    validate_mbr(props.delivery_mode, constants.AMPQ_DELIVERY_MODES, 'delivery mode', int)
    validate_uid(props.message_id, 'message_id')
    validate_int(props.timestamp, 'message timestamp')
    validate_mbr(props.type, constants.TYPES, 'message type')
    validate_mbr(props.user_id, constants.USERS, 'message user')

    # Validate headers.
    for header in {'producer_id', 'producer_version'}:
        if header not in props.headers:
            msg = "[{}] is a required header field.".format(header)
            raise ValueError(msg)
    validate_mbr(props.headers['producer_id'], constants.PRODUCERS, 'producer id')
    validate_vrs(props.headers['producer_version'], 'producer version')
