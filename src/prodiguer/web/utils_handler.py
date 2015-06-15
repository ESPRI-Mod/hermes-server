# -*- coding: utf-8 -*-

"""
.. module:: api.utils_handler.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: API web request handler utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.utils import config
from prodiguer.utils import logger
from prodiguer.utils.data_convertor import jsonify



# Base address to API endpoints.
_BASE_ADDRESS = 'http{0}://{1}/api/1'


def get_endpoint(ep):
    """Returns the endpoint prefixed with base adress.

    :param ep: Endpoint suffix.
    :type ep: str

    :returns: The endpoint prefixed with base adress.
    :rtype: str

    """
    ssl = config.web.ssl
    host = config.web.host.strip()
    if ssl:
        base_address =  _BASE_ADDRESS.format("s", host)
    else:
        base_address =  _BASE_ADDRESS.format("", host)

    return base_address + ep


def write_response(handler, error=None):
    """Writes a response.

    :param tornado.web.RequestHandler handler: An api handler.
    :param Exception error: Runtime error.

    """
    if error:
        handler.clear()
        data = {
            'status': 1,
            'error': unicode(error),
            'error_type': unicode(type(error))
            }
    else:
        try:
            data = handler.output
        except AttributeError:
            data = {}
        if 'status' not in data:
            data['status'] = 0

    write_json_response(handler, data)


def write_json_response(handler, data):
    """Writes an HTTP JSON response.

    :param tornado.web.RequestHandler handler: An api handler.
    :param dict data: Response data.

    """
    # Set HTTP header.
    handler.set_header("Content-Type", "application/json; charset=utf-8")

    # Write JSON response.
    handler.write(jsonify(data))


def write_csv_response(handler, error=None):
    """Writes a response in CSV format.

    :param tornado.web.RequestHandler handler: An api handler.
    :param Exception error: Runtime error.

    """
    # Write errors in default format.
    if error:
        write_response(handler, error)

    # Set CSV data.
    try:
        data = handler.output
    except AttributeError:
        data = ""

    # Set HTTP header.
    handler.set_header("Content-Type", "text/csv; charset=utf-8")

    # Write CSV response.
    handler.write(data)


def log(api_type, handler, error=None):
    """Logging utilty function.

    :param str action: Action being invoked.
    :param Exception error: Runtime error.

    """
    if error:
        msg = "{0} --> error --> {1} --> {2}"
        msg = msg.format(api_type, handler, error)
        logger.log_web_error(msg)
    else:
        msg = "{0} --> success --> {1}"
        msg = msg.format(api_type, handler)
        logger.log_web(msg)

