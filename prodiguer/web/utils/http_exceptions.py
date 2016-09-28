# -*- coding: utf-8 -*-

"""
.. module:: web.utils.exceptions.py
   :platform: Unix
   :synopsis: Custom exceptions used in this module for better readability of code.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""

class RequestValidationException(Exception):
    """Base class for request validation exceptions.

    """
    pass


class SecurityError(RequestValidationException):
    """Raised if a security issue arises.

    """
    def __init__(self, msg):
        """Instance constructor.

        """
        super(SecurityError, self).__init__(
            "SECURITY EXCEPTION :: {}".format(msg)
            )


class InvalidJSONSchemaError(RequestValidationException):
    """Raised if the submitted issue post data is invalid according to a JSON schema.

    """
    def __init__(self, json_errors):
        """Instance constructor.

        """
        super(InvalidJSONSchemaError, self).__init__(
            'ISSUE HAS INVALID JSON SCHEMA: \n{}'.format(json_errors))

