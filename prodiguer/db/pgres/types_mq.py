# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.types.py
   :platform: Unix
   :synopsis: Hermes message queue database tables.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime
import uuid

from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy import Unicode

from prodiguer.db.pgres.entity import Entity



# Database schema.
_SCHEMA = 'mq'


class Message(Entity):
    """Represents a message flowing through the MQ platform.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message'
    __table_args__ = (
        {'schema':_SCHEMA}
    )

    # Attributes.
    app_id = Column(Unicode(63), nullable=False)
    producer_id = Column(Unicode(63), nullable=False)
    producer_version = Column(Unicode(31), nullable=False)
    type_id = Column(Unicode(63), nullable=False)
    user_id = Column(Unicode(63), nullable=False)
    email_id = Column(BigInteger)
    uid = Column(Unicode(63),
                 nullable=False,
                 unique=True,
                 default=unicode(uuid.uuid4()))
    correlation_id_1 = Column(Unicode(63), nullable=True, index=True)
    correlation_id_2 = Column(Unicode(63), nullable=True)
    correlation_id_3 = Column(Unicode(63), nullable=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    timestamp_raw = Column(Unicode(63), nullable=False)
    content_encoding = Column(Unicode(63), nullable=True, default=u"utf-8")
    content_type = Column(Unicode(63),
                          nullable=True,
                          default=u"application/json")
    content = Column(Text, nullable=True)


class MessageEmail(Entity):
    """Represents an email received from a computing centre.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message_email'
    __table_args__ = (
        {'schema':_SCHEMA}
    )

    # Attributes.
    email_id = Column(BigInteger, nullable=False, unique=True)
    arrival_date = Column(DateTime)
    arrival_latency = Column(Integer)
    dispatch_date = Column(DateTime)


class MessageEmailStats(Entity):
    """Represents statisitics regarding an email received from a computing centre.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message_email_stats'
    __table_args__ = (
        {'schema':_SCHEMA}
    )

    # Attributes.
    email_id = Column(BigInteger, nullable=False)
    email_rejected = Column(Boolean, default=False)
    arrival_date = Column(DateTime)
    arrival_latency = Column(Integer)
    dispatch_date = Column(DateTime)
    incoming = Column(Integer)
    errors_decoding_base64 = Column(Integer)
    errors_decoding_json = Column(Integer)
    errors_encoding_ampq = Column(Integer)
    excluded = Column(Integer)
    outgoing = Column(Integer)
    outgoing_0000 = Column(Integer)
    outgoing_0100 = Column(Integer)
    outgoing_1000 = Column(Integer)
    outgoing_1001 = Column(Integer)
    outgoing_1100 = Column(Integer)
    outgoing_1900 = Column(Integer)
    outgoing_1999 = Column(Integer)
    outgoing_2000 = Column(Integer)
    outgoing_2100 = Column(Integer)
    outgoing_2900 = Column(Integer)
    outgoing_2999 = Column(Integer)
    outgoing_7000 = Column(Integer)
    outgoing_7010 = Column(Integer)
    outgoing_7011 = Column(Integer)
    outgoing_7100 = Column(Integer)
    outgoing_8888 = Column(Integer)
