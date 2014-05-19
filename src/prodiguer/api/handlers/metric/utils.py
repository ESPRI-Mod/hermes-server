# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.metric.utils.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric utility functions.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import datetime, re

from .... import db



# Regular expression for validating group name.
_GROUP_NAME_REGEX = '[^a-zA-Z0-9_-]'

# Min/max length of group name.
_GROUP_NAME_MIN_LENGTH = 4
_GROUP_NAME_MAX_LENGTH = 256

# Set of supported formats.
_FORMATS = ['json', 'csv']

# HTTP header names.
HTTP_HEADER_Access_Control_Allow_Origin = "Access-Control-Allow-Origin"


def _get_name(entity_type, id):
    """Utility function to map a db entity id to an entity name.

    :param db.types.Entity entity: Entity instance.

    :returns: Entity instance name.
    :rtype: str

    """
    return db.cache.get_name(entity_type, id)


def get_list(entity_type):
    """Returns a list of db entities formatted for front-end.

    :param db.types.Entity entity: Entity instance.

    :returns: A list of entites in dictionary format ready to be returned to front-end.
    :rtype: list
    
    """
    return [get_item(e) for e in db.dao.get_all(entity_type)]


def get_item(entity):
    """Returns a db entity formatted for front-end.

    :param db.types.Entity entity: Entity instance.

    :returns: Entity information in dictionary format ready to be returned to front-end.
    :rtype: dict
    
    """
    # Convert to a dictionary.
    d = db.types.Convertor.to_dict(entity)
    
    # Remove row meta-info.
    del d["row_create_date"]
    del d["row_update_date"]

    # Format date fields
    for k, v in d.items():
        if type(v) == datetime.datetime:
            d[k] = str(v)[:10]

    return d


def validate_group_name(name):
    """Validates a simulation metric group name.

    :param str name: A simulation metric group name.

    """
    def throw():
        raise ValueError("Invalid metric group name: {0}".format(name))        

    if re.compile(_GROUP_NAME_REGEX).search(name):
        throw()
    if len(name) < _GROUP_NAME_MIN_LENGTH or len(name) > _GROUP_NAME_MAX_LENGTH:
        throw()        


def validate_format(format):
    """Validates a simulation metric format.

    :param str format: A simulation metric format.

    """
    if format not in _FORMATS:
        raise ValueError("Unsupported metric group format: {0}".format(format))        

