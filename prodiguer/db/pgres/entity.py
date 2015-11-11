# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.utils_types.py
   :platform: Unix
   :synopsis: Domain model utility classes and functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import random

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base

from prodiguer.db.pgres import convertor
from prodiguer.db.pgres import session
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


    @classmethod
    def fetch_all(cls):
        """Fetches all instances from the database.

        """
        qry = session.query(cls)
        qry = qry.order_by(cls.id)

        return qry.all()


    @classmethod
    def fetch_by_id(cls, entity_id):
        """Fetches an instances from the database by its identifier.

        """
        qry = session.query(cls)
        qry = qry.filter(cls.id == entity_id)

        return qry.first()


    @classmethod
    def fetch_by_name(cls, entity_name):
        """Fetches an instances from the database by its name.

        """
        qry = session.query(cls)
        qry = qry.filter(cls.name == entity_name)

        return qry.first()


    @classmethod
    def fetch_count(cls):
        """Fetches count of all instances within the database.

        """
        qry = session.query(cls)

        return qry.count()


    @classmethod
    def fetch_random(cls):
        """Fetches a random instance from the database for testing purposes.

        """
        collection = cls.retrieve_all()
        if len(collection):
            return collection[random.randint(0, len(collection) - 1)]

        return None


    @classmethod
    def fetch_random_sample(cls):
        """Fetches a random sample from the database for testing purposes.

        """
        collection = cls.retrieve_all()
        if len(collection):
            return random.sample(collection, random.randint(1, len(collection)))

        return []


    @classmethod
    def persist(cls, hydrator, retriever):
        """Persists an instance to the database.

        :param function hydrator: Pointer to a function that will hydrate an instance.
        :param function retriever: Pointer to a function that will retrieve an instance.

        :returns: An instance hydrated with values and persisted to the database.
        :rtype: cls

        """
        instance = cls()
        hydrator(instance)
        try:
            session.insert(instance)
        except IntegrityError:
            session.rollback()
            instance = retriever()
            hydrator(instance)
            session.update(instance)

        return instance


# Mixin with sql alchemy.
Entity = declarative_base(metadata=METADATA, cls=_BaseEntity)
