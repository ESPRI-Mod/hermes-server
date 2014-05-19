# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.type_utils.py
   :platform: Unix
   :synopsis: Domain model utility classes and functions.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import datetime
import json
import random
import uuid

from dateutil import parser as date_parser
from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    inspect,
    Integer,
    MetaData,
    Unicode
    )
from sqlalchemy.ext.declarative import declarative_base

from .. utils import (
    convert, 
    runtime as rt
    )



# Set of supported types.
supported_types = None

# Common sqlalchemy metadata container.
metadata = MetaData()


def assert_type(etype):
    """Asserts that passed entity type is supported.

    :param etype: A supported entity type.
    :type etype: class

    """
    if etype not in supported_types:
        rt.throw('DB entity type is unknown :: {0}'.format(type))

        
def create_fk(fk, nullable=False):
    """Factory function to return a foreign key column.

    :param fk: Name of foreign key to be created.
    :type fk: str

    :param nullable: Flag indicating whether foreign key is nullable or not.
    :type nullable: bool

    :returns: A foreign key reference.
    :rtype: sqlalchemy.Column

    """
    return Column(Integer, ForeignKey(fk), nullable=nullable)


def parse_attr_value(v, expected_type):
    """Parses an attribute value."""
    if v.__class__ != expected_type:
        if expected_type is datetime.datetime:
            if (v.__class__ in (str, unicode)) and len(v):
                try:
                    v = date_parser.parse(str(v))
                except (ValueError, TypeError):
                    pass

    return v


class BaseEntity(object):
    """Base entity sub-classed from all Prodiguer db types.

    """
    # Entity attributes.
    id = Column(Integer, primary_key=True)


    def __init__(self):
        """Constructor.

        """
        super(BaseEntity, self).__init__()


    def __repr__(self):
        """Debugging representation.

        """
        return Convertor.to_string(self)


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



class BaseControlledVocabularyEntity(BaseEntity):
    """A controlled vocabulary base class.

    """
    # Attributes.
    code = Column(Unicode(63))
    description = Column(Unicode(511))


    @property
    def name(self):
        """Helper property for use with cache."""
        return self.code


# Mixin with sql alchemy.
Entity = declarative_base(metadata=metadata, cls=BaseEntity)
ControlledVocabularyEntity = declarative_base(metadata=metadata, cls=BaseControlledVocabularyEntity)



class Convertor(object):
    """Encapsulates all entity conversion functions.

    """
    @staticmethod
    def from_dict(etype, target):
        assert_type(etype)

        def _from_dict(d):
            i = etype()
            for k, v in d.items():                
                if v is not None and k != 'id':  
                    setattr(i, k, v)

            return i

        # Convert either an instance or sequence.
        if type(target) is list:
            return map(_from_dict, target)
        else:
            return _from_dict(target)


    @staticmethod
    def to_dict(target):
        """Returns a dictionary representation.

        :param target: Target to be converted to a dictionary.
        :type target: Entity | list

        :returns: Dictionary representation of target.
        :rtype: dict

        """
        if target is None:
            return {}

        def _to_dict(e):
            # Use sqlalchemy column mappings to derive dictionary keys.
            cols = inspect(e).mapper.columns

            # Return a dictionary comprehension.
            return { c.name: getattr(e, c.name) for c in cols }

        # Convert either an instance or sequence.
        try:
            return map(_to_dict, target)
        except TypeError:
            return _to_dict(target)


    @staticmethod
    def to_json(target, key_formatter=None):
        """Returns a json representation.

        :param target: Target to be converted to json.
        :type target: Entity | list

        :param key_formatter: A dictionary key formatter function.
        :type key_formatter: function | None

        :returns: Json representation of target.
        :rtype: unicode

        """
        return convert.dict_to_json(Convertor.to_dict(target), key_formatter)


    @staticmethod
    def to_string(target):
        """Returns a string representation.

        :param target: Target to be converted to a string.
        :type target: Entity | list

        :returns: String representation of target.
        :rtype: str

        """
        return str(Convertor.to_dict(target))

