# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_mq.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of message queue db types.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import datetime
import uuid

from sqlalchemy import (
    Column,
    DateTime,
    Text,
    Unicode
    )

from . type_utils import (
    create_fk,
    ControlledVocabularyEntity,
    Entity
    )



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

    # Foreign keys.
    app_id = create_fk('mq.tbl_message_application.id')
    producer_id = create_fk('mq.tbl_message_producer.id')
    type_id = create_fk('mq.tbl_message_type.id')

    # Attributes.
    uid = Column(Unicode(63), nullable=False, unique=True, default=unicode(uuid.uuid4()))
    timestamp = Column(DateTime, nullable=False, default=datetime.datetime.now)
    timestamp_raw = Column(Unicode(63), nullable=True)
    timestamp_precision = Column(Unicode(7), nullable=False, default=u"ms")
    content_encoding = Column(Unicode(63), nullable=True, default=u"utf-8")
    content_type = Column(Unicode(63), nullable=True, default=u"application/json")
    content = Column(Text, nullable=True)


class MessageApplication(ControlledVocabularyEntity):
    """Distinguishes the application that will process the message.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message_application'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )


class MessageProducer(ControlledVocabularyEntity):
    """Distinguishes the producer that originally dispatched the message.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message_producer'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )


class MessageType(ControlledVocabularyEntity):
    """Distinguishes the type of runtime message.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message_type'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )
