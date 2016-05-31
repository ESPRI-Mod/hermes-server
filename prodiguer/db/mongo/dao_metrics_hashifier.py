# -*- coding: utf-8 -*-

"""
.. module:: hermes_client/metrics/formatter/hashifier.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Sets metric hashes in order to uniquely identify one metric from another within a group.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import hashlib
import os



# Name of config fiel containing hash fieldset.
_HASH_FIELDSET_CONFIG_FILENAME = "dao_metrics_hashifier.config"

# Set of fields used to create a metric hash.
_HASH_FIELDSET = set()

# Name of metric id field.
_METRIC_ID = '_id'


def _init_hash_fieldset():
    """Initializes set of hash fields.

    """
    if _HASH_FIELDSET:
        return

    path = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(path, _HASH_FIELDSET_CONFIG_FILENAME)
    with open(path, 'r') as config_file:
        _HASH_FIELDSET.update([l.strip() for l in config_file.readlines() if l])


def _get_hashid(group_id, data):
    """Returns the hash identifier of a metric set.

    """
    hashid = unicode(group_id)
    for key in [k for k in sorted(data.keys()) if k in _HASH_FIELDSET]:
        hashid += unicode(key)
        hashid += unicode(data[key])

    return unicode(hashlib.md5(hashid).hexdigest())


def set_identifiers(group_id, data):
    """Computes hash identifiers for each set of metrics to be formatted.

    """
    _init_hash_fieldset()
    for item in data:
        item[_METRIC_ID] = _get_hashid(group_id, item)
