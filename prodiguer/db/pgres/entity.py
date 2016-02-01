# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.utils_types.py
   :platform: Unix
   :synopsis: Domain model utility classes and functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy.ext.declarative import declarative_base

from prodiguer.db.pgres import convertor
from prodiguer.db.pgres.meta import METADATA



class _BaseEntity(object):
    """Base entity sub-classed from all Prodiguer db types.

    """
    # Entity attributes.
    id = Column(Integer, primary_key=True)
    row_create_date = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    row_update_date = Column(DateTime, onupdate=datetime.datetime.utcnow)

    def __init__(self):
        """Constructor.

        """
        super(_BaseEntity, self).__init__()


    def __repr__(self):
        """Debugging representation.

        """
        return unicode(convertor.convert(self))


    @property
    def is_new(self):
        """Returns a flag indicating whether the entity instance is new or not.

        """
        return self.id is None


# Mixin with sql alchemy.
Entity = declarative_base(metadata=METADATA, cls=_BaseEntity)
