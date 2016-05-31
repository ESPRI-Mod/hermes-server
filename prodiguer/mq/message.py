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

from prodiguer.mq import constants
from prodiguer.mq import validator
from prodiguer.utils import config
from prodiguer.utils import convert



class Message(object):
    """Wraps a message either being consumed or produced."""
    def __init__(self, props, content, decode=False):
        """Constructor.

        :param pika.BasicProperties props: Set of AMPQ properties associated with the message.
        :param object content: Message content.
        :param bool decode: Flag indicating whether message payload is to be decoded.

        """
        validator.validate_ampq_basic_properties(props)

        self.abort = False
        self.content = content
        self.content_raw = content
        self.content_type = props.content_type
        self.exchange = constants.MESSAGE_TYPE_EXCHANGE[props.type]
        self.msg = None
        self.props = self.properties = props
        self.routing_key = "{0}.{1}.{2}.{3}.{4}".format(config.deploymentMode,
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
