# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.convertor.py
   :platform: Unix
   :synopsis: Converts prodiguer db types to dictionaries.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import sqlalchemy as sa



def convert(instance):
    """Converts mapped entity instance to a dictionary.

    :param object instance: Data to be converted.

    :returns: Converted data.
    :rtype: dict

    """
    return {c.name: getattr(instance, c.name) for c in sa.inspect(instance).mapper.columns}



def as_datetime_string(col):
    """Converts a column result into a datetime string.

    :param sqlalchemy.Column col: Collumn to be converted.

    :returns: The column wrapped in a date string conversion function.
    :rtype: sqlalchemy.Column

    """
    return sa.func.to_char(col, "YYYY-MM-DD HH24:MI:ss.US")


def as_date_string(col):
    """Converts a column result into a date string.

    :param sqlalchemy.Column col: Collumn to be converted.

    :returns: The column wrapped in a date string conversion function.
    :rtype: sqlalchemy.Column

    """
    return sa.func.to_char(col, "YYYY-MM-DD")
