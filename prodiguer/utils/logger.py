# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.utils.runtime.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Runtime utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import arrow



# Set of logging levels.
LOG_LEVEL_DEBUG = 'DUBUG'
LOG_LEVEL_INFO = 'INFO'
LOG_LEVEL_WARNING = 'WARNING'
LOG_LEVEL_ERROR = 'ERROR'
LOG_LEVEL_CRITICAL = 'CRITICAL'
LOG_LEVEL_FATAL = 'FATAL'

# Defaults.
_DEFAULT_APP = "PRODIGUER-SERVER"
_DEFAULT_MODULE = "**"
_DEFAULT_INSTITUTE = "IPSL"

# Text to display when passed a null message.
_NULL_MSG = "-------------------------------------------------------------------------------"


def _get_formatted_message(msg, module, level, app, institute):
    """Returns a message formatted for logging.

    """
    if msg is None:
        return _NULL_MSG
    else:
        return "{0} [{1}] :: {2} {3} > {4} : {5}".format(
            unicode(arrow.get())[0:-13],
            level,
            institute,
            app,
            module,
            unicode(msg).strip()
            )


def log(
    msg=None,
    module=_DEFAULT_MODULE,
    level=LOG_LEVEL_INFO,
    app=_DEFAULT_APP,
    institute=_DEFAULT_INSTITUTE
    ):
    """Outputs a message to log.

    :param str msg: Message to be written to log.
    :param str module: Module emitting log message (e.g. MQ).
    :param str level: Message level (e.g. INFO).
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    # TODO use structlog or other such library
    print(_get_formatted_message(msg, module, level, app, institute))


def log_error(
    err,
    module=_DEFAULT_MODULE,
    app=_DEFAULT_APP,
    institute=_DEFAULT_INSTITUTE
    ):
    """Logs a runtime error.

    :param ProdiguerClientException err: Error to be written to log.
    :param str module: Module emitting log message (e.g. MQ).
    :param str level: Message level (e.g. INFO).
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    msg = "!! {0} RUNTIME ERROR !! :: ".format(module)
    if issubclass(BaseException, err.__class__):
        msg += "{} :: ".format(err.__class__)
    msg += "{}".format(err)
    log(msg, module, LOG_LEVEL_ERROR, app, institute)


def log_cv(
    msg,
    level=LOG_LEVEL_INFO,
    app=_DEFAULT_APP,
    institute=_DEFAULT_INSTITUTE
    ):
    """Logs controlled vocabulary related events.

    :param str msg: Message for writing to log.
    :param str level: Message level (e.g. INFO).
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    log(msg, "CV", level, app, institute)


def log_cv_warning(msg, app=_DEFAULT_APP, institute=_DEFAULT_INSTITUTE
    ):
    """Logs controlled vocabulary warning related events.

    :param str msg: Message for writing to log.
    :param str level: Message level (e.g. INFO).
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    log_cv(msg, LOG_LEVEL_WARNING, app, institute)


def log_mq(
    msg,
    host=None,
    queue=None,
    level=LOG_LEVEL_INFO,
    app=_DEFAULT_APP,
    institute=_DEFAULT_INSTITUTE
    ):
    """Logs message queue related event.

    :param str msg: Message for writing to log.
    :param str host: MQ host name.
    :param str queue: MQ queue name.
    :param str level: Message level (e.g. INFO).
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    def _log(msg_):
        """Wraps outer function."""
        log(msg_, "MQ", level, app, institute)

    if host is None and queue is None:
        _log(msg)
    elif queue is None:
        _log("{0} :: {1}".format(msg, host))
    else:
        _log("{0} :: {1} :: {2}.".format(msg, host, queue))


def log_mq_error(err, app=_DEFAULT_APP, institute=_DEFAULT_INSTITUTE):
    """Logs a message queue runtime error.

    :param Exception err: Message queue processing error.
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    log_error(err, "MQ", app, institute)


def log_mq_warning(msg, app=_DEFAULT_APP, institute=_DEFAULT_INSTITUTE):
    """Logs a message queue warning related event.

    :param str msg: Message for writing to log.
    :param str level: Message level (e.g. INFO).
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    log_mq(msg, level=LOG_LEVEL_WARNING, app=app, institute=institute)


def log_db(
    msg,
    level=LOG_LEVEL_INFO,
    app=_DEFAULT_APP,
    institute=_DEFAULT_INSTITUTE
    ):
    """Logs database related events.

    :param str msg: Database message for writing to log.
    :param str level: Message level (e.g. INFO).
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    log(msg, "DB", level, app, institute)


def log_db_error(err, app=_DEFAULT_APP, institute=_DEFAULT_INSTITUTE):
    """Logs a runtime error.

    :param Exception err: Database processing error.
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    log_error(err, "DB", app, institute)


def log_web(
    msg,
    level=LOG_LEVEL_INFO,
    app=_DEFAULT_APP,
    institute=_DEFAULT_INSTITUTE
    ):
    """Logs web related events.

    :param str msg: Web application message for writing to log.
    :param str level: Message level (e.g. INFO).
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    log(msg, "WEB", level, app, institute)


def log_web_warning(msg, app=_DEFAULT_APP, institute=_DEFAULT_INSTITUTE):
    """Logs web warning events.

    :param str msg: Web application message for writing to log.
    :param str app: Application emitting log message (e.g. libIGCM).
    :param str institute: Institute emitting log message (e.g. libIGCM).

    """
    log_web(msg, LOG_LEVEL_WARNING, app, institute)


def log_web_error(err, app=_DEFAULT_APP, institute=_DEFAULT_INSTITUTE):
    """Logs a runtime error.

    :param err: Message for writing to log.
    :type err: Sub-class of BaseException

    """
    log_error(err, "WEB", app, institute)
