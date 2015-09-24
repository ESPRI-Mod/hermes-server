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
from sqlalchemy import Text
from sqlalchemy import Unicode

from prodiguer.db.pgres.entity import Entity



# Database schema.
SCHEMA = 'superviseur'


class Supervision(Entity):
    """Simulation supervision scripts.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_supervision'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Attributes.
    simulation_uid = Column(Unicode(63), nullable=False)
    job_uid = Column(Unicode(63), nullable=False)
    dispatch_date = Column(DateTime)
    script = Column(Text, nullable=True)
    script_encoding = Column(Unicode(63), nullable=True, default=u"utf-8")
    script_mime_type = Column(Unicode(63), nullable=True, default=u"text/plain")
    trigger_code = Column(Unicode(63), nullable=False)
    trigger_date = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
