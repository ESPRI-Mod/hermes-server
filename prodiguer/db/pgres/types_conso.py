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
_SCHEMA = 'conso'



class Allocation(Entity):
    """Resource allocation information.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_allocation'
    __table_args__ = (
        UniqueConstraint('centre', 'machine', 'node_type', 'project', 'start_date'),
        {'schema':_SCHEMA}
    )

    # Columns.
    centre = Column(Unicode(127), nullable=False)       # centre on which the project is active
    project = Column(Unicode(127), nullable=False)      # name of associated project
    sub_project = Column(Unicode(127), nullable=True)   # name of associated sub-project
    machine = Column(Unicode(127), nullable=False)      # machine on which there is a resource allocation
    node_type = Column(Unicode(127), nullable=False)    # architecture on which there is a resource allocation
    start_date = Column(DateTime, nullable=False)       # allocation start date
    end_date = Column(DateTime, nullable=False)         # allocation end date
    total_hrs = Column(Float, nullable=False)           # amount of the allocation (hours)


class Consumption(Entity):
    """Resource consumption information.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_consumption'
    __table_args__ = (
        UniqueConstraint('allocation_id', 'sub_project', 'login', 'date'),
        {'schema':_SCHEMA}
    )

    # Columns.
    allocation_id = Column(Integer,                     # ID of associated allocation
        ForeignKey('conso.tbl_allocation.id'),
        nullable=False)
    sub_project = Column(Unicode(127), nullable=True)   # name of associated sub-project
    login = Column(Unicode(127), nullable=True)         # login considered
    date = Column(DateTime, nullable=False)             # date considered
    total_hrs = Column(Float, nullable=False)           # amount of resources (hours) used at date for considered allocation


class CPUState(Entity):
    """CPU usage by allocation.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_cpu_state'
    __table_args__ = (
        {'schema':_SCHEMA}
    )

    # Columns.
    allocation_id = Column(Integer,                     # ID of associated allocation
        ForeignKey('conso.tbl_allocation.id'),
        nullable=False)
    date = Column(DateTime, nullable=False)             # date considered
    total_running = Column(Integer, nullable=False)     # number of cpu's in running state
    total_pending = Column(Integer, nullable=False)     # number of cpu's in pending state


class OccupationStore(Entity):
    """Data related to project data storage.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_occupation_store'
    __table_args__ = (
        {'schema':_SCHEMA}
    )

    centre = Column(Unicode(127), nullable=False)       # centre on which the project is active
    project = Column(Unicode(127), nullable=False)      # name of associated project
    typeof = Column(Unicode(7), nullable=False)         # type of occupation: work | store
    login = Column(Unicode(127), nullable=False)        # login considered
    date = Column(DateTime, nullable=False)             # date considered
    name = Column(Unicode(511), nullable=False)         # name of the storage space
    size_gb = Column(Float, nullable=False)             # space (GB) used by the considered login
