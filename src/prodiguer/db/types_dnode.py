# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_dnode.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of dnode db types.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Unicode,
    UniqueConstraint
    )
from sqlalchemy.orm import (
    backref,
    relationship
    )

from . type_utils import (
    create_fk,
    Entity
    )



# PostGres DB schema to which the types are attached.
_DB_SCHEMA = 'dnode'


class DataNode(Entity):
    """A logical node from which data services are accessible (e.g. OpenDap, GridFTP ... etc).

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_data_node'
    __table_args__ = (
        UniqueConstraint('institute_id', 'name'),
        {'schema' : _DB_SCHEMA}
    )

    # Foreign keys.
    institute_id = create_fk('shared.tbl_institute.id')

    # Relationships.
    data_servers = relationship("DataServer", backref="data_node")

    # Attributes.
    name = Column(Unicode(16), nullable=False)
    root_url = Column(Unicode(511), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    is_default = Column(Boolean, nullable=False, default=False)


class DataServer(Entity):
    """A server installed upon a data node (e.g. OpenDap, GridFTP ... etc).

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_data_server'
    __table_args__ = (
        {'schema' : _DB_SCHEMA}
    )

    # Foreign keys.
    data_node_id = create_fk('dnode.tbl_data_node.id')
    server_type_id = create_fk('dnode.tbl_data_server_type.id')

    # Attributes.
    base_url = Column(Unicode(511), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)


    @classmethod
    def get_default_sort_key(cls):
        """Gets default sort key.

        """
        return lambda x: x.base_url.upper()


class DataServerType(Entity):
    """A type of file server installed upon a data node (e.g. OpenDap, GridFTP ... etc).

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_data_server_type'
    __table_args__ = (
        {'schema' : _DB_SCHEMA}
    )

    # Attributes.
    name = Column(Unicode(255), nullable=False, unique=True)
    display_name = Column(Unicode(31), nullable=False)
    description = Column(Unicode(127), nullable=False)


class DRSComponent(Entity):
    """
    An component within a DRS schema.
    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_drs_component'
    __table_args__ = (
        UniqueConstraint('schema_id', 'name'),
        {'schema' : _DB_SCHEMA}
    )

    # Foreign keys.
    parent_component_id = create_fk('dnode.tbl_drs_component.id', nullable=True)
    schema_id = create_fk('dnode.tbl_drs_schema.id')

    # Relationships.
    elements = relationship("DRSElement", backref="component", lazy='joined')

    # Attributes.
    name = Column(Unicode(16), nullable=False)
    display_name = Column(Unicode(63), nullable=False)
    description = Column(Unicode(127), nullable=False)


    @property
    def active_elements(self):
        """Gets set of active drs elements.

        """
        for e in self.elements:
            if e.is_active == True:
                yield e


    def get_element(self, name):
        """Gets first element with matching name.

        """
        if self.elements is not None:
            for e in self.elements:
                if e.name.upper() == name.upper():
                    return e
        return None


class DRSElement(Entity):
    """A controlled vocabulary member within a DRS component.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_drs_element'
    __table_args__ = (
        UniqueConstraint('component_id', 'name'),
        {'schema' : _DB_SCHEMA}
    )

    # Foreign keys.
    component_id = create_fk('dnode.tbl_drs_component.id')

    # Attributes.
    name = Column(Unicode(16), nullable=False)
    description = Column(Unicode(127))
    home_page_url = Column(Unicode(511))
    info1 = Column(Unicode(511))
    info2 = Column(Unicode(511))
    info3 = Column(Unicode(511))
    info4 = Column(Unicode(511))
    is_from_vocab = Column(Boolean, nullable=False, default=False)
    is_default = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False, default=False)


class DRSElementMapping(Entity):
    """
    A mapping between two DRS items.
    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_drs_element_mapping'
    __table_args__ = (
        {'schema' : _DB_SCHEMA}
    )

    # Foreign keys.
    from_id = create_fk('dnode.tbl_drs_element.id')
    to_id = create_fk('dnode.tbl_drs_element.id')


class DRSSchema(Entity):
    """A type of DRS schema supported with the Prodiguer platform.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_drs_schema'
    __table_args__ = (
        {'schema' : _DB_SCHEMA}
    )

    # Relationships.
    components = relationship("DRSComponent", backref="schema", lazy='joined')

    # Attributes.
    name = Column(Unicode(255), nullable=False, unique=True)
    description = Column(Unicode(127), nullable=False)
    home_page_url = Column(Unicode(511))
    is_active = Column(Boolean, nullable=False, default=True)


    @property
    def root_component(self):
        """Gets schema root component.

        """
        for c in self.components:
            if c.parent_component_id is None:
                return c
        return None


    def get_component(self, name):
        """Gets first component with matching name.

        """
        for c in self.components:
            if c.name.upper() == name.upper():
                return c
        return None


    def get_elements(self, component, get_active_only=True):
        """Gets set of drs elements.

        """
        c = self.get_component(component)
        if c is not None and c.elements is not None:
            if get_active_only == True:
                return DRSElement.get_sorted(c.active_elements)
            else:
                return DRSElement.get_sorted(c.elements)
        return None


    def get_element(self, component, element):
        """Gets a drs element within schema.

        """
        c = self.get_component(component)
        if c is not None:
            return c.get_element(element)
        return None
