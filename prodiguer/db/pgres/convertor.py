# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.convertor.py
   :platform: Unix
   :synopsis: Converts ORM type instances to / from various representations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import inspect as sa_inspect

from prodiguer.utils.convert import dict_to_json



def from_dict(etype, target):
    """Returns a dictionary converted to either an entity instance or collection.

    :param class etype: Type of entity to be returned.
    :param list | dict target: Target to be converted.

    :returns: Target converted to either an entity instance or collection.
    :rtype: list | Entity

    """
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
        cols = sa_inspect(e).mapper.columns

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


def to_json(target, key_formatter=None):
    """Returns a json representation.

    :param target: Target to be converted to json.
    :type target: Entity | list

    :param key_formatter: A dictionary key formatter function.
    :type key_formatter: function | None

    :returns: Json representation of target.
    :rtype: unicode

    """
    return dict_to_json(to_dict(target), key_formatter)


def to_string(target):
    """Returns a string representation.

    :param target: Target to be converted to a string.
    :type target: Entity | list

    :returns: String representation of target.
    :rtype: str

    """
    return str(to_dict(target))


def to_unicode(target):
    """Returns a unicode representation.

    :param target: Target to be converted to a unicode.
    :type target: Entity | list

    :returns: Unicode representation of target.
    :rtype: unicode

    """
    return unicode(to_dict(target))
