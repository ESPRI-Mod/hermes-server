# -*- coding: utf-8 -*-

"""
.. module:: api.utils_handler.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: API web request handler utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.httputil

from prodiguer.utils import config
from prodiguer.utils import logger
from prodiguer.utils.data_convertor import jsonify



# Base address to API endpoints.
_BASE_ADDRESS = 'http{0}://{1}/api/1'

# Http response codes.
HTTP_RESPONSE_BAD_REQUEST = 400


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


def log_response(handler, error=None):
    """Logs a response.

    :param tornado.web.RequestHandler handler: An api handler.
    :param Exception error: Runtime error.

    """
    api_type = str(handler).split(".")[2]
    if error:
        msg = "{0} --> error --> {1} --> {2}"
        msg = msg.format(api_type, handler, error)
        logger.log_web_error(msg)
    else:
        msg = "{0} --> success --> {1}"
        msg = msg.format(api_type, handler)
        logger.log_web(msg)


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


def is_vanilla_request(handler):
    """Returns a flag indicating whether the request has no associated body, query or files.

    """
    return (bool(handler.request.body) or
            bool(handler.request.query) or
            bool(handler.request.files)) == False


def validate_vanilla_request(handler):
    """Simple HTTP request validator for endpoints that do not expect any parameters.

    :param tornado.web.RequestHandler handler: Endpoint handler.

    """
    # Invalid request if it has query, body or files.
    if (bool(handler.request.body) or
        bool(handler.request.query) or
        bool(handler.request.files)):
        raise tornado.httputil.HTTPInputError()


def validate_request(
    handler,
    body_validator=None,
    files_validator=None,
    query_validator=None
    ):
    """Validates an incoming HTTP request.

    :param tornado.web.RequestHandler handler: Endpoint handler.
    :param function body_validator: Validator to apply over request body.
    :param function body_validator: Validator to apply over request body.
    :param function files_validator: Validator to apply over request files.
    :param function query_validator: Validator to apply over request query.

    """
    def _validate(validator, target):
        """Applies a validator otherwise verifies target is undefined.

        """
        if validator is not None:
            validator(handler)
        elif bool(target):
            raise tornado.httputil.HTTPInputError()

    _validate(body_validator, handler.request.body)
    _validate(files_validator, handler.request.files)
    _validate(query_validator, handler.request.query)


def invoke(handler, validation_tasks, processing_tasks, error_tasks=[]):
    """Invokes handler tasks.

    """
    def _invoke_task(task):
        """Invokes an individual task.

        """
        try:
            task()
        except TypeError as err:
            if err.message.find("takes exactly 1 argument (0 given)") > -1:
                task(handler)
            else:
                raise err

    def _get_tasks(taskset, extend=True):
        """Returns formatted & extended taskset.

        """
        try:
            iter(taskset)
        except TypeError:
            taskset = [taskset]
        if extend:
            taskset.append(lambda err=None: write_response(handler, err))
            taskset.append(lambda err=None: log_response(handler, err))

        return taskset

    # Execute validation tasks - N.B. exceptions are bubbled up.
    for task in _get_tasks(validation_tasks, False):
        _invoke_task(task)

    # Execute processing tasks.
    for task in _get_tasks(processing_tasks):
        try:
            _invoke_task(task)
        # Execute error tasks.
        except Exception as err:
            try:
                for task in _get_tasks(error_tasks):
                    task(err)
            except:
                pass
            # N.B. breaks out of green line.
            break
