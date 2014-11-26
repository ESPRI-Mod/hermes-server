# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.cache.py
   :copyright: Copyright "Jun 12, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Static cache of database entities.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import random

from . import dao, types
from .. utils import runtime as rt



# Cache.
_cache = {}


def load():
    """Loads cache.

    """
    if len(_cache) == 0:
        for collection_key in types.CACHEABLE:
            _cache[collection_key] = dao.get_all(collection_key)
        rt.log_db("LOADED CACHE ...")


def reload():
    """Loads cache.

    """
    rt.log_db("RELOADING CACHE ...")
    for collection_key in _cache.keys():
        del _cache[collection_key]
    load()


def _is_matched_name(item, name):
    """Predicate determining whether an item has a matching name.

    """
    name = str(name).upper()
    names = [item.name]
    try:
        names += item.synonyms.split(",")
    except AttributeError:
        pass
    else:
        names = [n for n in names if n]
        names = [n.strip() for n in names]
        names = [n for n in names if len(n)]
    names = [n.upper() for n in names]

    return name in names


def exists(collection_key, item_key):
    """Determines whether a cache item exists or not.

    :param class collection_key: Cached collection key.
    :param int|str item_key: Cache item identifier.

    :returns: True if item is cached, False otherwise.
    :rtype: bool

    """
    # JIT load.
    load()

    if collection_key not in _cache:
        return False
    if item_key is None:
        return False
    if len(str(item_key).strip()) == 0:
        return False

    collection = _cache[collection_key]
    if isinstance(item_key, int):
        return len([i for i in collection if i.id == item_key]) == 1
    else:
        return  len([i for i in collection if _is_matched_name(i, item_key)]) == 1


def get_collection(collection_key):
    """Returns a cached collection.

    :param class collection_key: Cached collection key.

    :returns: A Prodiguer collection if cached, None otherwise.
    :rtype: list | None

    """
    if collection_key in _cache:
        return _cache[collection_key]


def get_count():
    """Returns count of items in cache.

    :returns: Count of items in cache.
    :rtype: int

    """
    count = 0
    for collection_key in _cache.keys():
        count += len(_cache[collection_key])

    return count


def get_items():
    """Returns all cached items.

    :returns: All cached items.
    :rtype: list

    """
    result = []
    for collection_key in _cache.keys():
        for item in _cache[collection_key]:
            result.append(item)

    return result


def get_item(collection_key, item_key):
    """Returns a cached item.

    :param class|str collection_key: Cached entity type key.
    :param int|str item_key: Cached item key.

    :returns: A Prodiguer entity if item is cached, None otherwise.
    :rtype: A subclass of prodiguer.types.Entity

    """
    # JIT load.
    load()

    # Escape if passed invalid key.
    if not exists(collection_key, item_key):
        return None

    # Set target collection.
    collection = _cache[collection_key]

    # Filter collection.
    if isinstance(item_key, int):
        collection = [i for i in collection if i.id == item_key]
    else:
        collection = [i for i in collection if _is_matched_name(i, item_key)]

    return collection[0] if collection else None


def get_name(collection_key, item_key):
    """Returns a cached item name.

    :param class|str collection_key: Cached entity type key.
    :param int|str item_key: Cached item key.

    :returns: A Prodiguer entity name if item is cached, None otherwise.
    :rtype: str | None

    """
    # JIT load.
    load()

    item = get_item(collection_key, item_key)

    return None if item is None else item.name


def get_id(collection_key, item_key):
    """Returns a cached item id.

    :param class|str collection_key: Cached entity type key.
    :param int|str item_key: Cached item key.

    :returns: A Prodiguer entity identifier if item is cached, None otherwise.
    :rtype: int | None

    """
    # JIT load.
    load()

    item = get_item(collection_key, item_key)

    return None if item is None else item.id


def get_random(collection_key):
    """Returns a random cache item.

    :param class collection_key: Cached collection key.

    :returns: A cached item selected at random.
    :rtype: Sub-class of types.Entity

    """
    # JIT load.
    load()

    collection = _cache[collection_key]
    item_id = random.randint(0, len(collection) - 1)

    return collection[item_id]


def get_random_name(collection_key):
    """Returns a random cache item name.

    :param class collection_key: Cached collection key.

    :returns: The name of a cached item selected at random.
    :rtype: str

    """
    return get_random(collection_key).name


def get_random_id(collection_key):
    """Returns a random cache item id.

    :param class collection_key: Cached collection key.

    :returns: The id of a cached item selected at random.
    :rtype: int

    """
    return get_random(collection_key).id


