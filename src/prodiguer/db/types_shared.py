# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_shared.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of db types shared across other schemas.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from sqlalchemy import (
    Boolean,
    Column,
    Integer,
    Unicode
    )
from sqlalchemy.orm import relationship

from . type_utils import (
    create_fk,
    Entity
    )


# PostGres schema to which the types are attached.
_DB_SCHEMA = 'shared'



class Activity(Entity):
    """An activity within the context of which a set of experiments are to be run.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_activity'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Foreign keys.
    drs_schema_id = create_fk('dnode.tbl_drs_schema.id', True)

    # Relationships.
    experiments = relationship("Experiment", backref="activity", lazy='joined')
    experiment_groups = relationship("ExperimentGroup", backref="activity", lazy='joined')

    # Attributes.
    name = Column(Unicode(255), nullable=False, unique=True)
    description = Column(Unicode(127), nullable=False)
    home_page_url = Column(Unicode(511), nullable=False)
    is_default = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False, default=True)


class Institute(Entity):
    """A climate modelling institute.

    """
    # Sqlalchemy directives.
    __tablename__ = 'tbl_institute'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Relationships.
    compute_nodes = relationship("ComputeNode", backref="institute")
    data_nodes = relationship("DataNode", backref="institute")

    # Attributes.
    name = Column(Unicode(255), nullable=False, unique=True)
    long_name = Column(Unicode(127), nullable=False)
    home_page_url = Column(Unicode(1023))
