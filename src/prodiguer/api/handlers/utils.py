# -*- coding: utf-8 -*-

"""
.. module:: utils.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Web request handler utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import json

from bson import json_util

from prodiguer.utils import config, rt



# Base address to API endpoints.
_BASE_ADDRESS = 'http{0}://{1}{2}/api/1'


def get_endpoint(ep):
    """Returns the endpoint prefixed with base adress.

    :param ep: Endpoint suffix.
    :type ep: str

    :returns: The endpoint prefixed with base adress.
    :rtype: str

    """
    ssl = config.api.ssl
    host = config.api.host.strip()
    try:
        port = config.api.port.strip()
    except AttributeError:
        port = config.api.port
    if port:
        port = ":{0}".format(port)

    if ssl:
        base_address =  _BASE_ADDRESS.format("s", host, port)
    else:
        base_address =  _BASE_ADDRESS.format("", host, port)

    return base_address + ep


def write(handler, error=None):
    """Writes a response.

    :param tornado.web.RequestHandler handler: An api handler.
    :param Exception error: Runtime error.

    """
    # Write errors as json.
    if error:
        handler.clear()
        handler.write({
            'status': 1,
            'error': unicode(error)
            })
        return

    # Write output as json.
    output = handler.output if hasattr(handler, 'output') else {}
    if 'status' not in output:
        output['status'] = 0
    handler.set_header("Content-Type", "application/json; charset=utf-8")
    handler.write(json.dumps(output, default=json_util.default))


def write_csv(handler, error=None):
    """Writes a response in CSV format.

    :param tornado.web.RequestHandler handler: An api handler.
    :param Exception error: Runtime error.

    """
    # Write errors as json.
    if error:
        write(handler, error)

    # Write output as CSV.
    output = handler.output if hasattr(handler, 'output') else ""
    handler.set_header("Content-Type", "text/csv; charset=utf-8")
    handler.write(output)


def log(api_type, handler, error=None):
    """Logging utilty function.

    :param str action: Action being invoked.
    :param Exception error: Runtime error.

    """
    if error:
        msg = "{0} --> error --> {1} --> {2}"
        msg = msg.format(api_type, handler, error)
        rt.log_api_error(msg)
    else:
        msg = "{0} --> success --> {1}"
        msg = msg.format(api_type, handler)
        rt.log_api(msg)

