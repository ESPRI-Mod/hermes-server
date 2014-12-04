# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_shared.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of db types shared across other schemas.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import (
    Boolean,
    Column,
    Enum,
    Integer,
    Unicode
    )
from sqlalchemy.orm import relationship

from . type_utils import (
    create_fk,
    Entity
    )


# PostGres schema to which the types are attached.
_DOMAIN_PARTITION = 'shared'



class Activity(Entity):
    """An activity within the context of which a set of experiments are to be run.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_activity'
    __table_args__ = (
        {'schema':_DOMAIN_PARTITION}
    )

    # Attributes.
    name = Column(Unicode(255), nullable=False, unique=True)
    description = Column(Unicode(127), nullable=False)
    home_page_url = Column(Unicode(511), nullable=False)
    is_default = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False, default=True)
    is_reviewed = Column(Boolean, nullable=False, default=True)


class Institute(Entity):
    """A climate modelling institute.

    """
    # Sqlalchemy directives.
    __tablename__ = 'tbl_institute'
    __table_args__ = (
        {'schema':_DOMAIN_PARTITION}
    )

    # Relationships.
    compute_nodes = relationship("ComputeNode", backref="institute")

    # Attributes.
    name = Column(Unicode(255), nullable=False, unique=True)
    long_name = Column(Unicode(127), nullable=False)
    home_page_url = Column(Unicode(1023))
    is_reviewed = Column(Boolean, nullable=False, default=True)


# CV collection types.
CV_TYPE_ACTIVITY = u"activity"
CV_TYPE_INSTITUTE = u"institute"
CV_TYPE_COMPUTE_NODE = u"compute_node"
CV_TYPE_COMPUTE_NODE_LOGIN = u"compute_node_login"
CV_TYPE_COMPUTE_NODE_MACHINE = u"compute_node_machine"
CV_TYPE_EXPERIMENT = u"experiment"
CV_TYPE_EXPERIMENT_GROUP = u"experiment_group"
CV_TYPE_MESSAGE_TYPE = u"message_type"
CV_TYPE_MESSAGE_APPLICATION = u"message_application"
CV_TYPE_MESSAGE_PRODUCER = u"message_producer"
CV_TYPE_MODEL = u"model"
CV_TYPE_MODEL_FORCING = u"model_forcing"
CV_TYPE_SIMULATION_SPACE = u"simulation_space"
CV_TYPE_SIMULATION_STATE = u"simulation_state"

# CV typeset.
CV_TYPES = [
    CV_TYPE_ACTIVITY,
    CV_TYPE_INSTITUTE,
    CV_TYPE_COMPUTE_NODE,
    CV_TYPE_COMPUTE_NODE_LOGIN,
    CV_TYPE_COMPUTE_NODE_MACHINE,
    CV_TYPE_EXPERIMENT,
    CV_TYPE_EXPERIMENT_GROUP,
    CV_TYPE_MESSAGE_TYPE,
    CV_TYPE_MESSAGE_APPLICATION,
    CV_TYPE_MESSAGE_PRODUCER,
    CV_TYPE_MODEL,
    CV_TYPE_MODEL_FORCING,
    CV_TYPE_SIMULATION_SPACE,
    CV_TYPE_SIMULATION_STATE,
]

# CV collection type enum.
ControlledVocabularyTypeEnum = \
    Enum(CV_TYPE_ACTIVITY,
         CV_TYPE_INSTITUTE,
         CV_TYPE_COMPUTE_NODE,
         CV_TYPE_COMPUTE_NODE_LOGIN,
         CV_TYPE_COMPUTE_NODE_MACHINE,
         CV_TYPE_EXPERIMENT,
         CV_TYPE_EXPERIMENT_GROUP,
         CV_TYPE_MESSAGE_TYPE,
         CV_TYPE_MESSAGE_APPLICATION,
         CV_TYPE_MESSAGE_PRODUCER,
         CV_TYPE_MODEL,
         CV_TYPE_MODEL_FORCING,
         CV_TYPE_SIMULATION_SPACE,
         CV_TYPE_SIMULATION_STATE,
         schema=_DOMAIN_PARTITION,
         name='ControlledVocabularyTypeEnum')


class CvTerm(Entity):
    """A controlled vocabulary term.

    """
    # Sqlalchemy directives.
    __tablename__ = 'tbl_cvterm'
    __table_args__ = (
        {'schema':_DOMAIN_PARTITION}
    )

    # Attributes.
    cv_type = Column(ControlledVocabularyTypeEnum, nullable=False)
    name = Column(Unicode(255), nullable=False)
    synonyms = Column(Unicode(2047))
    description = Column(Unicode(1023))
    url = Column(Unicode(1023))
    sort_key = Column(Unicode(511))
    is_active = Column(Boolean, nullable=False, default=True)
    is_reviewed = Column(Boolean, nullable=False, default=True)
