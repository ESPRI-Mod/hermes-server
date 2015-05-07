# -*- coding: utf-8 -*-

"""
.. module:: pyesdoc.utils.decorators.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of library function decorators.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""

def validate(validator):
    """Validation function decorator.

    """
    def decorate(func):
        """The decorator."""
        def wrapper(*args, **kwargs):
            """The wrapper."""
            validator(*args, **kwargs)
            return func(*args, **kwargs)
        return wrapper
    return decorate

