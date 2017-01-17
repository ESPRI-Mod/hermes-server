# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.mongo.dao_metrics.py
   :platform: Unix
   :synopsis: Set of metrics related data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from collections import OrderedDict

import pymongo
from bson.objectid import ObjectId

from prodiguer.db.mongo import dao_metrics_hashifier as hashifier
from prodiguer.db.mongo import utils



# Name of pymongo db in which metrics are stored.
_DB_NAME = "metrics"

# Token of object identifier that each inserted record is associated with.
_MONGO_OBJECT_ID = "_id"



def _format_group_id(group_id):
    """Returns a formatted group id.

    """
    return None if not group_id else group_id.strip().lower()


def _fetch(action, query=None):
    """Fetches data form db.

    """
    return action(query or {})


def _get_collection(group_id):
    """Returns a MongoDB collection pointer.

    """
    group_id = _format_group_id(group_id)

    return utils.get_db_collection(_DB_NAME, group_id, OrderedDict)


def add(group_id, metrics, duplicate_action):
    """Persists a set of metrics to the database.

    :param str group_id: ID of the metric group being added.
    :param list metrics: Set of metric being added.
    :param str duplicate_action: Action to take when encountering a metric set with a duplicate hash identifier.

    :returns: Set of line id's of newly inserted metrics.
    :rtype: list

    """
    # Set target db collection.
    collection = _get_collection(group_id)

    # Insert metrics.
    duplicates = []
    for metric in metrics:
        try:
            collection.insert(metric)
        except pymongo.errors.DuplicateKeyError:
            duplicates.append(metric)
            if duplicate_action == 'force':
                collection.remove({_MONGO_OBJECT_ID: metric[_MONGO_OBJECT_ID]})
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

    """
    collection = _get_collection(group_id)
    if query:
        collection.remove(query)
    else:
        collection.drop()


def delete_sets(group_id, set_ids):
    """Deletes sets of metrics.

    :param str group_id: ID of a metric group.
    :param list set_ids: Setf of metric identifiers.

    :returns: Original and updated line counts.
    :rtype: tuple

    """
    collection = _get_collection(group_id)

    count = collection.count()
    for set_id in set_ids:
        collection.remove({_MONGO_OBJECT_ID: set_id})

    return count, collection.count()


def exists(group_id):
    """Returns a flag indicating whether metrics collection already exists.

    :param str group_id: ID of the metric group being added.

    :returns: True if collection exists false otherwise.
    :rtype: bool

    """
    group_id = _format_group_id(group_id)

    return group_id in fetch_list()


def fetch(group_id, query=None):
    """Returns a group of metrics.

    :param str group_id: ID of the metric group being returned.
    :param dict query: Query filter to be applied.

    :returns: A group of metrics.
    :rtype: dict

    """
    collection = _get_collection(group_id)
    cursor = _fetch(collection.find, query)

    return list(cursor)


def fetch_columns(group_id, exclude_id_column=False):
    """Returns set of column names associated with a group of metrics.

    :param str group_id: ID of a metric group.
    :param bool exclude_id_column: Flag indicating whether the ID column should be excluded or not.

    :returns: Set of column names associated with a group of metrics.
    :rtype: list

    """
    # Get columns from first record.
    collection = _get_collection(group_id)
    cursor = _fetch(collection.find_one)
    columns = list(cursor) if cursor else []

    # Split columns into user defined and control.
    user_columns = [k for k in columns if not k.startswith('_')]
    ctl_columns = [k for k in columns if k.startswith('_')]

    # Exclude id column.
    if exclude_id_column and _MONGO_OBJECT_ID in ctl_columns:
        ctl_columns.remove(_MONGO_OBJECT_ID)

    return sorted(user_columns) + sorted(ctl_columns)


def fetch_count(group_id, query=None):
    """Returns count of number of metrics within a group.

    :param str group_id: ID of a metric group.
    :param dict query: Query filter to be applied.

    :returns: Count of number of metrics within a group.
    :rtype: int

    """
    collection = _get_collection(group_id)
    cursor = _fetch(collection.find, query)

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
    fields = fetch_columns(group_id, False)
    collection = _get_collection(group_id)
    cursor = _fetch(collection.find, query)

    return [sorted(cursor.distinct(f)) for f in fields if f != _MONGO_OBJECT_ID]


def rename(group_id, new_group_id):
    """Renames an existing group of metrics.

    :param str group_id: ID of a metric group.
    :param str new_group_id: New ID of the metric group.

    """
    new_group_id = _format_group_id(new_group_id)
    collection = _get_collection(group_id)
    collection.rename(new_group_id)


def set_hashes(group_id):
    """Resets the hash identifiers of an existing group of metrics.

    :param str group_id: ID of a metric group.

    """
    hashifier.set_identifiers(group_id, fetch(group_id))
