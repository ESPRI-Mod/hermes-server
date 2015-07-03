# -*- coding: utf-8 -*-

"""
.. module:: web.utils.request_validation.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Web service request validation utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.httputil



# HTTP header - Content-Type.
_HTTP_HEADER_CONTENT_TYPE = "Content-Type"


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


def validate(
    handler,
    body_validator=None,
    cookies_validator=None,
    files_validator=None,
    headers_validator=None,
    query_validator=None
    ):
    """Validates an incoming HTTP request.

    :param tornado.web.RequestHandler handler: Endpoint handler.
    :param function body_validator: Validator to apply over request body.
    :param function cookies_validator: Validator to apply over request cookies.
    :param function files_validator: Validator to apply over request files.
    :param function headers_validator: Validator to apply over request headers.
    :param function query_validator: Validator to apply over request query.

    """
    def _validate(validator, target):
        """Applies a validator otherwise verifies target is undefined.

        """
        if validator is not None:
            try:
                validator(handler)
            except TypeError:
                validator()
        elif bool(target):
            raise tornado.httputil.HTTPInputError("Bad request")

    _validate(query_validator, handler.request.query)
    _validate(cookies_validator, handler.request.cookies)
    _validate(body_validator, handler.request.body)
    _validate(files_validator, handler.request.files)


def validate_content_type(handler, expected_types):
    """Validates HTTP Content-Type request header.

    :param tornado.web.RequestHandler handler: Endpoint handler.
    :param set expected_types: Expected http content types.


    """
    if _HTTP_HEADER_CONTENT_TYPE not in handler.request.headers:
        raise ValueError("Content-Type is undefined")
    header = handler.request.headers[_HTTP_HEADER_CONTENT_TYPE]
    if not header in expected_types:
        raise ValueError("Unsupported content-type")
