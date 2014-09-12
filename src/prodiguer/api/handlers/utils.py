# -*- coding: utf-8 -*-

# Module imports.
from ... utils import (
    config,
    runtime as rt
    )



# Base address to API endpoints.
_BASE_ADDRESS = 'http://{0}:{1}/api/1'


def get_endpoint(ep):
    """Returns the endpoint prefixed with base adress.

    :param ep: Endpoint suffix.
    :type ep: str

    :returns: The endpoint prefixed with base adress.
    :rtype: str

    """
    return _BASE_ADDRESS.format(config.api.host, config.api.port) + ep


def write(handler, error=None, format='json'):
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

    # Write json.
    elif format == 'json':
        output = handler.output if handler.output else {}
        if 'status' not in output:
            output['status'] = 0
        handler.write(output)  # NOTE - tornado automatically sets content-type = json for dicts

    # Write csv.
    elif format == 'csv':
        handler.set_header('Content-Type', 'application/csv; charset=utf-8')
        handler.write(handler.output if handler.output else "")


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

