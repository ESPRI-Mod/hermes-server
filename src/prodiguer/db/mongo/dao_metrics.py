# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.mongo.dao_metrics.py
   :platform: Unix
   :synopsis: Set of metrics related data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os, hashlib
from collections import OrderedDict

import pymongo
from bson.objectid import ObjectId

from prodiguer.db.mongo import utils



# Name of pymongo db in which metrics are stored.
_DB_NAME = "metrics"

# Token that mongo injected fields are preffixed with.
_MONGO_FIELD_PREFIX = "_"

# Token of object identifier that each inserted record is associated with.
_MONGO_OBJECT_ID = "_id"

# Token of collection index: hash id.
_IDX_HASH_ID = u"idx_hash_id"

# Default field limiter to use when querying a collection.
_DEFAULT_FIELD_LIMITER = {
    _MONGO_OBJECT_ID: 0
}

# Name of config fiel containing hash fieldset.
_HASH_FIELDSET_CONFIG_FILENAME = "hash_fieldset.config"

# Set of fields used to create a metric hash.
_HASH_FIELDSET = set()

# Name of hash id field.
_HASH_FIELDNAME = '_hashID'


def _format_group_id(group_id):
    """Returns a formatted group id.

    """
    return None if not group_id else group_id.strip().lower()


def _fetch(action, include_db_id=True, query=None):
    """Fetches data form db.

    """
    # Parse params.
    query = query or {}
    field_limiter = _DEFAULT_FIELD_LIMITER if not include_db_id else None

    # Return data.
    if field_limiter:
        return action(query, field_limiter, as_class=OrderedDict)
    else:
        return action(query, as_class=OrderedDict)


def _init_indexes(group_id):
    """Initialiszes metric group db indexes.

    :param str group_id: ID of a metric group.

    """
    group_id = _format_group_id(group_id)
    collection = utils.get_db_collection(_DB_NAME, group_id)

    # Create hash id index to enforce row uniqueness.
    if _IDX_HASH_ID not in collection.index_information():
        collection.create_index(_HASH_FIELDNAME, name=_IDX_HASH_ID, unique=True)


def _init_hash_fieldset():
    """Initializes set of hash fields.

    """
    if _HASH_FIELDSET:
        return

    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, _HASH_FIELDSET_CONFIG_FILENAME)
    with open(path, 'r') as config_file:
        _HASH_FIELDSET.update([l.strip() for l in config_file.readlines() if l])


def _get_hash(group_id, data):
    """Returns the hash of a metric set.

    """
    hashid = unicode(_format_group_id(group_id))
    for key in [k for k in data.keys() if k in _HASH_FIELDSET]:
        hashid += unicode(key)
        hashid += unicode(data[key])

    return unicode(hashlib.md5(hashid).hexdigest())


def add(group_id, metrics, duplicate_action):
    """Persists a set of metrics to the database.

    :param str group_id: ID of the metric group being added.
    :param list metrics: Set of metric being added.
    :param str duplicate_action: Action to take when encountering a metric set with a duplicate hash identifier.

    :returns: Set of line id's of newly inserted metrics.
    :rtype: list

    """
    # Initialize indexes.
    _init_indexes(group_id)

    # Set target db collection.
    group_id = _format_group_id(group_id)
    collection = utils.get_db_collection(_DB_NAME, group_id)

    # Insert metrics.
    duplicates = []
    for metric in metrics:
        try:
            collection.insert(metric)
        except pymongo.errors.DuplicateKeyError:
            duplicates.append(metric)
            if duplicate_action == 'force':
                collection.remove({ _HASH_FIELDNAME: metric[_HASH_FIELDNAME] })
                collection.insert(metric)

    # Return inserted & duplicates.
    if duplicate_action == 'force':
        return metrics, duplicates
    else:
        return [m for m in metrics if m not in duplicates], duplicates


def delete(group_id, query=None):
    """Deletes a group of metrics.

    :param str group_id: ID of a metric group to be deleted.
    :param dict query: Query filter to be applied.

    :returns: Names of remaining metric groups.
    :rtype: list

    """
    group_id = _format_group_id(group_id)
    collection = utils.get_db_collection(_DB_NAME, group_id)
    if query:
        collection.remove(query)
    else:
        collection.drop()


def delete_lines(group_id, line_ids):
    """Deletes a group of metrics.

    :param str group_id: ID of a metric group.
    :param list line_ids: ID's of individual lines within a metric group.

    :returns: Original and updated line counts.
    :rtype: tuple

    """
    group_id = _format_group_id(group_id)
    collection = utils.get_db_collection(_DB_NAME, group_id)

    count = collection.count()
    for line_id in line_ids:
        collection.remove({_MONGO_OBJECT_ID: ObjectId(line_id)})

    return count, collection.count()


def exists(group_id):
    """Returns a flag indicating whether metrics collection already exists.

    :param str group_id: ID of the metric group being added.

    :returns: True if collection exists false otherwise.
    :rtype: bool

    """
    group_id = _format_group_id(group_id)

    return group_id in fetch_list()


def fetch(group_id, include_db_id, query=None):
    """Returns a group of metrics.

    :param str group_id: ID of the metric group being returned.
    :param bool include_db_id: Flag indicating whether to include db id.
    :param dict query: Query filter to be applied.

    :returns: A group of metrics.
    :rtype: dict

    """
    group_id = _format_group_id(group_id)
    collection = utils.get_db_collection(_DB_NAME, group_id)
    cursor = _fetch(collection.find, include_db_id=include_db_id, query=query)

    return list(cursor)


def fetch_columns(group_id, include_db_id):
    """Returns set of column names associated with a group of metrics.

    :param str group_id: ID of a metric group.
    :param bool include_db_id: Flag indicating whether to include db id.

    :returns: Set of column names associated with a group of metrics.
    :rtype: list

    """
    group_id = _format_group_id(group_id)
    collection = utils.get_db_collection(_DB_NAME, group_id)
    cursor = _fetch(collection.find_one, include_db_id)

    return cursor.keys() if cursor else []


def fetch_count(group_id, query=None):
    """Returns count of number of metrics within a group.

    :param str group_id: ID of a metric group.
    :param dict query: Query filter to be applied.

    :returns: Count of number of metrics within a group.
    :rtype: int

    """
    group_id = _format_group_id(group_id)
    collection = utils.get_db_collection(_DB_NAME, group_id)
    cursor = _fetch(collection.find, query=query)

    return cursor.count()


def fetch_list():
    """Returns set of groups within metrics database.

    :returns: set of groups within metrics database.
    :rtype: list

    """
    mg_db = utils.get_db(_DB_NAME)

    groups = mg_db.collection_names()
    try:
        groups.remove('system.indexes')
    except ValueError:
        pass

    return sorted(groups)


def fetch_setup(group_id, query=None):
    """Returns setup data associated with a group of metrics.

    The setup data is the set of unique values for each field within the metric group.

    :param str group_id: ID of a metric group.
    :param dict query: Query filter to be applied.

    :returns: Setup data associated with a group of metrics.
    :rtype: dict

    """
    group_id = _format_group_id(group_id)
    query = query or {}
    collection = utils.get_db_collection(_DB_NAME, group_id)
    cursor = _fetch(collection.find, query=query)
    fields = fetch_columns(group_id, False)

    return [sorted(cursor.distinct(f)) for f in fields]


def rename(group_id, new_group_id):
    """Renames an existing group of metrics.

    :param str group_id: ID of a metric group.
    :param str new_group_id: New ID of the metric group.

    """
    group_id = _format_group_id(group_id)
    new_group_id = _format_group_id(new_group_id)
    collection = utils.get_db_collection(_DB_NAME, group_id)
    collection.rename(new_group_id)


def set_hashes(group_id):
    """Resets the hash identifiers of an existing group of metrics.

    :param str group_id: ID of a metric group.

    """
    _init_hash_fieldset()
    for metric in fetch(group_id, True):
        print "BEFORE", metric.get(_HASH_FIELDNAME)
        metric[_HASH_FIELDNAME] = _get_hash(group_id, metric)
        print "AFTER", metric.get(_HASH_FIELDNAME)
