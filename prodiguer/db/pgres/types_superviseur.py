# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_superviseur.py
   :platform: Unix
   :synopsis: Prodiguer superviseur database tables.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy import Unicode

from prodiguer.db.pgres.entity import Entity



# Database schema.
_SCHEMA = 'superviseur'


class Supervision(Entity):
    """Simulation supervision scripts.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_supervision'
    __table_args__ = (
        {'schema':_SCHEMA}
    )

    # Attributes.
    simulation_uid = Column(Unicode(63), nullable=False)
    job_uid = Column(Unicode(63), nullable=False)
    dispatch_date = Column(DateTime)
    dispatch_error = Column(Text)
    dispatch_try_count = Column(Integer, default=0)
    script = Column(Text, nullable=True)
    trigger_code = Column(Unicode(63), nullable=False)
    trigger_date = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
