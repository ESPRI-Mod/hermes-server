# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.monitoring.utils.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

from prodiguer import db



def get_list(entity_type):
    """Returns a list of db entities formatted for front-end.

    :param db.types.Entity entity: Entity instance.

    :returns: A list of entites in dictionary format ready to be returned to front-end.
    :rtype: list

    """
    collection = [get_item(e) for e in db.dao.get_all(entity_type)]

    return collection


def get_sorted_list(entity_type, key='name'):
    """Returns a sorted list of db entities formatted for front-end.

    :param db.types.Entity entity: Entity instance.
    :param expression key: Collection sort key.

    :returns: A sorted list of entites in dictionary format ready to be returned to front-end.
    :rtype: list

    """
    collection = get_list(entity_type)

    return sorted(collection, key=lambda instance: instance[key].lower())


def format_date_fields(obj):
    """Formats date fields within passed dictionary.

    """
    for key, val in obj.items():
        if type(val) == datetime.datetime:
            obj[key] = str(val)[:10]


def get_item(instance):
    """Returns a db entity formatted for front-end.

    :param db.types.Entity entity: Entity instance.

    :returns: Entity information in dictionary format ready to be returned to front-end.
    :rtype: dict

    """
    # Convert to a dictionary.
    obj = db.types.Convertor.to_dict(instance)

    # Set name attribute if required.
    if 'name' not in obj:
        try:
            obj['name'] = instance.name
        except AttributeError:
            pass

    # Remove row meta-info.
    del obj["row_create_date"]
    del obj["row_update_date"]

    # Format date fields
    format_date_fields(obj)

    return obj
