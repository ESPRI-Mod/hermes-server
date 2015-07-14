# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types.py
   :platform: Unix
   :synopsis: Prodiguer message queue db schema types.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime
import uuid

from sqlalchemy import BigInteger
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Text
from sqlalchemy import Unicode

from prodiguer.db.pgres.entity import Entity



# Database schema.
SCHEMA = 'mq'


class Message(Entity):
    """Represents a message flowing through the MQ platform.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Attributes.
    app_id = Column(Unicode(63), nullable=False)
    producer_id = Column(Unicode(63), nullable=False)
    type_id = Column(Unicode(63), nullable=False)
    user_id = Column(Unicode(63), nullable=False)
    uid = Column(Unicode(63),
                 nullable=False,
                 unique=True,
                 default=unicode(uuid.uuid4()))
    correlation_id_1 = Column(Unicode(63), nullable=True)
    correlation_id_2 = Column(Unicode(63), nullable=True)
    correlation_id_3 = Column(Unicode(63), nullable=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    timestamp_raw = Column(Unicode(63), nullable=True)
    timestamp_precision = Column(Unicode(7), nullable=False, default=u"ms")
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
        {'schema':SCHEMA}
    )

    # Attributes.
    uid = Column(BigInteger, nullable=False, unique=True)

