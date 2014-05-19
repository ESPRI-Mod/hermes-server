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
    publisher_id = create_fk('mq.tbl_message_publisher.id')
    type_id = create_fk('mq.tbl_message_type.id')

    # Attributes.
    uid = Column(Unicode(63), nullable=False, default=unicode(uuid.uuid4()))
    timestamp = Column(DateTime, nullable=True, default=datetime.datetime.now)
    content_encoding = Column(Unicode(63), nullable=False, default=u"utf-8")
    content_type = Column(Unicode(63), nullable=False, default=u"application/json")
    content = Column(Text, nullable=True)


class MessageApplication(ControlledVocabularyEntity):
    """Distinguishes the application that will process the message.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message_application'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )


class MessagePublisher(ControlledVocabularyEntity):
    """Distinguishes the publisher that originally sent the message.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message_publisher'
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
