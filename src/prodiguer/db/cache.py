# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.cache.py
   :copyright: Copyright "Jun 12, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Static cache of database entities.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import random

from . import dao, types



# Cache.
_cache = {}


def load():
    """Loads cache.

    """
    if len(_cache) == 0:
        for typeof in types.CACHEABLE:
            _cache[typeof] = dao.get_all(typeof)


def exists(typeof, key):
    """Determines whether a cache item exists or not.

    :param class typeof: Cache entity type.
    :param int|str key: Cache item identifier.

    :returns: True if item is cached, False otherwise.
    :rtype: bool

    """
    # JIT load.
    load()

    if typeof not in _cache or key is None:
        return False
    elif isinstance(key, int):
        return len([i for i in _cache[typeof] if i.id == key]) == 1
    else:
        key = str(key).upper()
        return len([i for i in _cache[typeof] if i.name.upper() == key]) == 1


def get_collection(typeof):
    """Returns a cached collection.

    :param class typeof: Cache entity type.

    :returns: A Prodiguer entity collection if cached, None otherwise.
    :rtype: list | None

    """
    if typeof in _cache:
        return _cache[typeof]


def get_item(typeof, key):
    """Returns a cached item.

    :param class typeof: Cache entity type.
    :param int|str key: Cache item identifier.

    :returns: A Prodiguer entity if item is cached, None otherwise.
    :rtype: A subclass of prodiguer.types.Entity

    """
    # JIT load.
    load()

    if not exists(typeof, key):
        return None
    elif isinstance(key, int):
        return [i for i in _cache[typeof] if i.id == key][0]
    else:
        key = str(key).upper()
        return [i for i in _cache[typeof] if i.name.upper() == key][0]


def get_name(typeof, key):
    """Returns a cached item name.

    :param class typeof: Cache entity type.
    :param int|str key: Cache item identifier.

    :returns: A Prodiguer entity ID if item is cached, None otherwise.
    :rtype: str | None

    """
    # JIT load.
    load()

    item = get_item(typeof, key)

    return None if item is None else item.name


def get_id(typeof, key):
    """Returns a cached item id.

    :param class typeof: Cache entity type.
    :param int|str key: Cache item identifier.

    :returns: A Prodiguer entity id if item is cached, None otherwise.
    :rtype: int | None

    """
    # JIT load.
    load()

    item = get_item(typeof, key)

    return None if item is None else item.id


def get_random(typeof):
    """Returns a random cache item.

    :param class typeof: Cache entity type.

    :returns: A random item from the cache.
    :rtype: Sub-class of types.Entity

    """
    # JIT load.
    load()

    return _cache[typeof][random.randint(0, len(_cache[typeof]) - 1)]


def get_random_name(typeof):
    """Returns a random cache item name.

    :param class typeof: Cache entity type.

    :returns: The name of a random cached item.
    :rtype: str

    """
    return get_random(typeof).name


def get_random_id(typeof):
    """Returns a random cache item id.

    :param class typeof: Cache entity type.

    :returns: The id of a random cached item.
    :rtype: int

    """
    return get_random(typeof).id
