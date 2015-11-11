# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.utils_types.py
   :platform: Unix
   :synopsis: Domain model utility classes and functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime
import random

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Integer
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base

from prodiguer.db.pgres import convertor
from prodiguer.db.pgres import session
from prodiguer.db.pgres import validator_dao as my_validator
from prodiguer.db.pgres.meta import METADATA
from prodiguer.utils import decorators



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
        return True if self.id is None else False


    @classmethod
    def _delete(cls, filter_expression=None):
        """Deletes instances from the database.

        """
        qry = session.query(cls)
        if filter_expression:
            qry = qry.filter(filter_expression)

        qry.delete()


    @classmethod
    def delete_all(cls):
        """Deletes all instances from the database.

        """
        cls._delete()


    @classmethod
    def delete_by_id(cls, entity_id):
        """Deletes an instances from the database by id.

        """
        cls._delete(cls.id == entity_id)


    @classmethod
    def delete_by_name(cls, entity_name):
        """Deletes an instances from the database by name.

        """
        cls._delete(cls.name == entity_name)


    @classmethod
    def _fetch(cls, filter_expression, fetch_iterable):
        """Fetches instances from the database.

        """
        qry = session.query(cls)
        if filter_expression:
            qry = qry.filter(filter_expression)


        return qry.all() if fetch_iterable else qry.first()


    @classmethod
    @decorators.validate(my_validator.validate_get_all)
    def fetch_all(cls):
        """Fetches all instances from the database.

        """
        return cls._fetch(None, True)


    @classmethod
    def fetch_by_id(cls, entity_id):
        """Fetches an instances from the database by its identifier.

        """
        return cls._fetch(cls.id == entity_id, False)


    @classmethod
    def fetch_by_name(cls, entity_name):
        """Fetches an instances from the database by its name.

        """
        return cls._fetch(cls.name == entity_name, False)


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
        collection = cls.fetch_all()
        if len(collection):
            return collection[random.randint(0, len(collection) - 1)]

        return None


    @classmethod
    def fetch_random_sample(cls):
        """Fetches a random sample from the database for testing purposes.

        """
        collection = cls.fetch_all()
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
