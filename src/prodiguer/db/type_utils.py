# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.type_utils.py
   :platform: Unix
   :synopsis: Domain model utility classes and functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

from dateutil import parser as date_parser
from sqlalchemy import (
    Column,
    ForeignKey,
    inspect,
    Integer,
    MetaData
    )
from sqlalchemy.ext.declarative import declarative_base

from prodiguer.utils import convert



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
        raise TypeError('DB entity type is unknown :: {0}'.format(type))


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


# Mixin with sql alchemy.
Entity = declarative_base(metadata=metadata, cls=BaseEntity)


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
    def to_dict(target, drop_db_admin_cols=False):
        """Returns a dictionary representation.

        :param target: Target to be converted to a dictionary.
        :type target: Entity | list

        :returns: Dictionary representation of target.
        :rtype: dict

        """
        if target is None:
            return {}

        def _to_dict(e):
            """Converts entity instance to a dictionary."""
            # Use sqlalchemy column mappings to derive dictionary keys.
            cols = inspect(e).mapper.columns

            # Return a dictionary comprehension.
            as_dict = { c.name: getattr(e, c.name) for c in cols }

            # Optionally drop db admin columns.
            if drop_db_admin_cols:
                del as_dict["row_create_date"]
                del as_dict["row_update_date"]

            return as_dict

        # Convert instance | sequence.
        try:
            iter(target)
        except TypeError:
            return _to_dict(target)
        else:
            return [_to_dict(i) for i in target]


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
