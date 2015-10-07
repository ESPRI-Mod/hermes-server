# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.type_convertor.py
   :platform: Unix
   :synopsis: Converts prodiguer db types to dictionaries.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import sqlalchemy as sa



def convert(instance):
    """Converts instance to a dictionary.

    :param object instance: Data to be converted.

    :returns: Converted data.
    :rtype: dict

    """
    return {c.name: getattr(instance, c.name) for c in sa.inspect(instance).mapper.columns}
