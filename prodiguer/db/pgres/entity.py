# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.utils_types.py
   :platform: Unix
   :synopsis: Domain model utility classes and functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy.ext.declarative import declarative_base


from prodiguer.db.pgres import convertor
from prodiguer.db.pgres.meta import METADATA



class _BaseEntity(object):
    """Base entity sub-classed from all Prodiguer db types.

    """
    # Entity attributes.
    id = Column(Integer, primary_key=True)


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
        return True if self.id is None else False


    @classmethod
    def get_default_sort_key(cls):
        """Gets default sort key.

        """
        if hasattr(cls, 'name'):
            return lambda x: "" if x.name is None else x.name.upper()
        elif hasattr(cls, 'ordinal_position'):
            return lambda x: x.ordinal_position
        elif hasattr(cls, 'code'):
            return lambda x: "" if x.code is None else x.code.upper()
        else:
            lambda x: x.id


    @classmethod
    def get_sorted(cls, collection, sort_key=None):
        """Gets sorted collection of instances.

        """
        if sort_key is None:
            sort_key=cls.get_default_sort_key()

        return sorted(collection, key=sort_key)


# Mixin with sql alchemy.
Entity = declarative_base(metadata=METADATA, cls=_BaseEntity)
