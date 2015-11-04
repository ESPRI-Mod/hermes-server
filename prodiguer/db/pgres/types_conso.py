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



class Allocation(Entity):
    """Resource allocation information.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_allocation'
    __table_args__ = (
        UniqueConstraint('centre', 'machine', 'node_type', 'project', 'start_date'),
        {'schema':SCHEMA}
    )

    # Columns.
    centre = Column(Unicode(127), nullable=False)       # centre on which the project is active
    end_date = Column(DateTime, nullable=False)         # allocation end date
    machine = Column(Unicode(127), nullable=False)      # machine on which there is a resource allocation
    node_type = Column(Unicode(127), nullable=False)    # architecture on which there is a resource allocation
    project = Column(Unicode(127), nullable=False)      # name of associated project
    start_date = Column(DateTime, nullable=False)       # allocation start date
    total_hrs = Column(Float, nullable=False)           # amount of the allocation (hours)


class Consumption(Entity):
    """Resource consumption information.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_consumption'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Columns.
    allocation_id = Column(Integer, ForeignKey('conso.tbl_allocation.id'))
    date = Column(DateTime, nullable=False)             # date considered
    total_hrs = Column(Float, nullable=False)           # amount of resources (hours) used at date for considered allocation
    login = Column(Unicode(127), nullable=True)         # login considered


class OccupationStore(Entity):
    """Data related to project data storage.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_occupation_store'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    date = Column(DateTime, nullable=False)             # date considered
    login = Column(Unicode(127), nullable=False)        # login considered
    name = Column(Unicode(127), nullable=False)         # name of the storage space
    size_gb = Column(Float, nullable=False)             # space (GB) used by the considered login
