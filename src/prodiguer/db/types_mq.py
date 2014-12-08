# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_mq.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of message queue db types.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime
import uuid

from sqlalchemy import (
    Column,
    DateTime,
    Text,
    Unicode
    )

from . type_utils import Entity



# PostGres schema to which the types are attached.
_DB_SCHEMA = 'mq'



class Message(Entity):
    """Represents a runtime messsage.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Attributes.
    app_id = Column(Unicode(63), nullable=False)
    producer_id = Column(Unicode(63), nullable=False)
    type_id = Column(Unicode(63), nullable=False)
    user_id = Column(Unicode(63), nullable=False)
    mode = Column(Unicode(15), nullable=True)
    uid = Column(Unicode(63),
                 nullable=False,
                 unique=True,
                 default=unicode(uuid.uuid4()))
    correlation_id_1 = Column(Unicode(63), nullable=True)
    correlation_id_2 = Column(Unicode(63), nullable=True)
    correlation_id_3 = Column(Unicode(63), nullable=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.datetime.now)
    timestamp_raw = Column(Unicode(63), nullable=True)
    timestamp_precision = Column(Unicode(7), nullable=False, default=u"ms")
    content_encoding = Column(Unicode(63), nullable=True, default=u"utf-8")
    content_type = Column(Unicode(63),
                          nullable=True,
                          default=u"application/json")
    content = Column(Text, nullable=True)
