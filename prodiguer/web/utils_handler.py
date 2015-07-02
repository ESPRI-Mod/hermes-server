# -*- coding: utf-8 -*-

"""
.. module:: api.utils_handler.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: API web request handler utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import inspect

import tornado
import tornado.httputil

from prodiguer.utils import config
from prodiguer.utils import logger
from prodiguer.utils.data_convertor import jsonify



# Base address to API endpoints.
_BASE_ADDRESS = '{0}/api/1{1}'

# Http response codes.
_HTTP_RESPONSE_BAD_REQUEST = 400
_HTTP_RESPONSE_SERVER_ERROR = 500



class ProdiguerWebServiceRequestHandler(tornado.web.RequestHandler):
    """A web service request handler.

    """
    @property
    def _can_return_debug_info(self):
        """Gets flag indicating whether the application can retrun debug information.

        """
        return self.application.settings.get('debug', False)


    def invoke(
        self,
        validation_taskset,
        processing_taskset,
        processing_error_taskset=[]
        ):
        """Invokes handler tasks.

        """
        def _write(data):
            """Writes HTTP response data.

            """
            # Set HTTP header.
            self.set_header("Content-Type", "application/json; charset=utf-8")

            # Write JSON response.
            self.write(jsonify(data))


        def _write_error(http_status_code, err):
            """Writes error response.

            """
            self.clear()
            reason = unicode(err) if self._can_return_debug_info else None
            self.send_error(http_status_code, reason=reason)


        def _write_invalid_request(err):
            """Writes request validation error.

            """
            _write_error(_HTTP_RESPONSE_BAD_REQUEST, err)


        def _write_success():
            """Writes processing success to response stream.

            """
            try:
                data = self.output
            except AttributeError:
                data = {}
            if 'status' not in data:
                data['status'] = 0
            _write(data)


        def _write_failure(err):
            """Writes processing failure to response stream.

            """
            _write_error(_HTTP_RESPONSE_BAD_REQUEST, err)


        def _log_success():
            """Logs a successful response.

            """
            api_type = str(self).split(".")[2]
            msg = "{0} --> success --> {1}"
            msg = msg.format(api_type, self)
            logger.log_web(msg)


        def _log_error(error):
            """Logs an error response.

            :param Exception error: Runtime error.

            """
            api_type = str(self).split(".")[2]
            msg = "{0} --> error --> {1} --> {2}"
            msg = msg.format(api_type, self, error)
            logger.log_web_error(msg)


        def _get_taskset(taskset):
            """Returns formatted & extended taskset.

            """
            try:
                iter(taskset)
            except TypeError:
                return [taskset]
            return taskset


        def _invoke(task, err=None):
            """Invokes a task.

            """
            if err is None:
                if len(inspect.getargspec(task)[0]) == 1:
                    task(self)
                else:
                    task()
            else:
                if len(inspect.getargspec(task)[0]) == 2:
                    task(self, err)
                else:
                    task(err)


        def _invoke_taskset(taskset, error_taskset):
            """Invokes a set of tasks.

            """
            for task in taskset:
                try:
                    _invoke(task)
                except Exception as err:
                    try:
                        for error_task in error_taskset:
                            _invoke(error_task, err)
                    # ... suppress inner exceptions.
                    except Exception:
                        pass
                    return err


        # Validate request.
        taskset = _get_taskset(validation_taskset)
        error_taskset = [_log_error, _write_invalid_request]
        error = _invoke_taskset(taskset, error_taskset)
        if error:
            return

        # Process request.
        taskset = _get_taskset(processing_taskset)
        taskset.append(_log_success)
        taskset.append(_write_success)
        error_taskset = _get_taskset(processing_error_taskset)
        error_taskset.append(_log_error)
        error_taskset.append(_write_failure)
        _invoke_taskset(taskset, error_taskset)


def get_endpoint(ep):
    """Returns the endpoint prefixed with base adress.

    :param str ep: Endpoint suffix.

    :returns: The endpoint prefixed with base adress.
    :rtype: str

    """
    return _BASE_ADDRESS.format(config.web.url, ep)


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

    # Set HTTP header.
    handler.set_header("Content-Type", "application/json; charset=utf-8")

    # Write JSON response.
    handler.write(jsonify(data))



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
