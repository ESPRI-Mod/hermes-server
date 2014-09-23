# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.message.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: A message wrapper.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>

"""
import base64

import pika

from . import constants
from ..utils import convert



class Message(object):
    """Wraps a message either being consumed or produced."""
    def __init__(self, props, content, exchange=None, decode=False):
        """Constructor.

        :param pika.BasicProperties props: Set of AMPQ properties associated with the message.
        :param object content: Message content.
        :param str exchange: An AMPQ message exchange.
        :param bool decode: Flag indicating whether message payload is to be decoded.

        """
        # Validate inputs.
        if not isinstance(props, pika.BasicProperties):
            raise ValueError("AMPQ message basic properties is invalid.")
        if 'mode' not in props.headers:
            msg = "[mode] is a required header field."
            raise ValueError(msg)
        if props.headers['mode'] not in constants.MODES:
            msg = "Unsupported mode: {0}."
            raise ValueError(msg.format(props.headers['mode']))
        if 'producer_id' not in props.headers:
            msg = "[producer_id] is a required header field."
            raise ValueError(msg)
        if props.headers['producer_id'] not in constants.PRODUCERS:
            msg = "Unsupported producer_id: {0}."
            raise ValueError(msg.format(props.headers['producer_id']))

        self.content = content
        self.content_raw = content
        self.content_type = props.content_type
        self.exchange = exchange
        self.props = self.properties = props
        self.routing_key = "{0}.{1}.{2}.{3}.{4}".format(props.headers['mode'],
                                                        props.user_id,
                                                        props.headers['producer_id'],
                                                        props.app_id,
                                                        props.type).lower()
        if decode:
            self.decode()


    def decode(self):
        """Decodes message content."""
        def _json(content):
            try:
                return convert.json_to_dict(content)
            except ValueError as err:
                raise Exception("json encoding error:\n{0}".format(content))

        def _base64(content):
            try:
                return base64.b64decode(content)
            except TypeError as err:
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
