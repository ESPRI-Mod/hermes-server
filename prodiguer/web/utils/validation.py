# -*- coding: utf-8 -*-

"""
.. module:: api._utils_validation.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Web service request validation utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.httputil



def Sequence(expected_type, expected_length=1):
    """Validates a sequence of query parameter values.

    """
    def f(val):
        """Inner function.

        """
        # Validate sequence length.
        if len(val) != expected_length:
            raise ValueError("Invalid request")

        # Validate sequence type.
        for item in val:
            try:
                expected_type(item)
            except ValueError:
                raise ValueError("Invalid request")

        return val

    return f


def validate_request(
    handler,
    body_validator=None,
    cookies_validator=None,
    files_validator=None,
    query_validator=None
    ):
    """Validates an incoming HTTP request.

    :param tornado.web.RequestHandler handler: Endpoint handler.
    :param function body_validator: Validator to apply over request body.
    :param function cookies_validator: Validator to apply over request cookies.
    :param function files_validator: Validator to apply over request files.
    :param function query_validator: Validator to apply over request query.

    """
    def _validate(validator, target):
        """Applies a validator otherwise verifies target is undefined.

        """
        if validator is not None:
            validator(handler)
        elif bool(target):
            raise tornado.httputil.HTTPInputError("Bad request")

    _validate(body_validator, handler.request.body)
    _validate(cookies_validator, handler.request.cookies)
    _validate(files_validator, handler.request.files)
    _validate(query_validator, handler.request.query)
