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
from sqlalchemy import Unicode

from prodiguer.db.pgres.entity import Entity



# Database schema.
SCHEMA = 'conso'


class Project(Entity):
    """A project being tracked for resource consumption.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_project'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Columns.
    name = Column(Unicode(127), nullable=False, unique=True)
    centre = Column(Unicode(127), required=True)
    machine = Column(Unicode(127), required=True)
    node_type = Column(Unicode(127), required=True)
    allocation = Column(Unicode(127), required=True)
    start_date = Column(DateTime, required=True)
    end_date = Column(DateTime, required=True)


class Consumption(Entity):
    """Resource consumption statistical data per centre.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_consumption'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    #TODO


class ConsumptionByLogin(Entity):
    """Resource consumption statistical data per login.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_consumption_by_login'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Columns.
    project = Column(Unicode(127), required=True)
    login = Column(Unicode(127), required=True)
    date = Column(DateTime, required=True)
    total = Column(Float, required=True)


class OccupationStore(Entity):
    """Data related to project data storage.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_occupation_store'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    project = Column(Unicode(127), required=True)
    login = Column(Unicode(127), required=True)
    date = Column(DateTime, required=True)
    store_name = Column(Unicode(127), required=True)
    store_size = Column(Float, required=True)
