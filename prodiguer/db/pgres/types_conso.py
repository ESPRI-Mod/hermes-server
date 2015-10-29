# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_conso.py
   :platform: Unix
   :synopsis: Resource consumption database tables.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Unicode
from sqlalchemy import UniqueConstraint

from prodiguer.db.pgres.entity import Entity



# Database schema.
SCHEMA = 'conso'


class Project(Entity):
    """A project being tracked for resource consumption.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_project'
    __table_args__ = (
        UniqueConstraint('name', 'centre', 'machine', 'node_type', 'start_date'),
        {'schema':SCHEMA}
    )

    # Columns.
    name = Column(Unicode(127), nullable=False)
    centre = Column(Unicode(127), nullable=False)
    machine = Column(Unicode(127), nullable=False)
    node_type = Column(Unicode(127), nullable=False)
    allocation = Column(Unicode(127), nullable=False)
    start_date = Column(DateTime, nullable=False)
    end_date = Column(DateTime, nullable=False)


class ConsumptionByProject(Entity):
    """Resource consumption statistical data by project.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_consumption_by_project'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Columns.
    project_id = Column(Integer, ForeignKey('conso.tbl_project.id'))
    date = Column(DateTime, nullable=False)
    total = Column(Float, nullable=False)


class ConsumptionByLogin(Entity):
    """Resource consumption statistical data by project & login.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_consumption_by_login'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Columns.
    project_id = Column(Integer, ForeignKey('conso.tbl_project.id'))
    login = Column(Unicode(127), nullable=False)
    date = Column(DateTime, nullable=False)
    total = Column(Float, nullable=False)


class OccupationStore(Entity):
    """Data related to project data storage.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_occupation_store'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    login = Column(Unicode(127), nullable=False)
    date = Column(DateTime, nullable=False)
    store_name = Column(Unicode(127), nullable=False)
    store_size = Column(Float, nullable=False)
