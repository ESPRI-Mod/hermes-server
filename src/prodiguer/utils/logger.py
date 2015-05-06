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

# Default logging settings.
_DEFAULT_MODULE = "**"
_DEFAULT_APP = "PRODIGUER"
_DEFAULT_INSTITUTE = "IPSL"


def log(msg=None, module=_DEFAULT_MODULE, level=LOG_LEVEL_INFO, app=_DEFAULT_APP, institute=_DEFAULT_INSTITUTE):
    """Outputs a message to log.

    :param msg: Message for writing to log.
    :type msg: str

    """
    # Format.
    if msg is not None:
        msg = "{0} :: {1} {2} {3} {4} > {5}".format(
            unicode(arrow.get()), institute, app, level, module, str(msg).strip())

    else:
        msg = "-------------------------------------------------------------------------------"

    # TODO output to logs.
    print msg


def log_error(err,
              module=_DEFAULT_MODULE,
              app=_DEFAULT_APP,
              institute=_DEFAULT_INSTITUTE,
              format_err=True):
    """Logs a runtime error.

    :param err: Message for writing to log.
    :type err: Sub-class of BaseException

    """
    if format_err:
        msg = "{0} :: {1}.".format(err.__class__, err)
    else:
        msg = err
    log(msg, module=module, level=LOG_LEVEL_ERROR, app=app, institute=institute)


def log_cv(msg,
           level=LOG_LEVEL_INFO,
           app=_DEFAULT_APP,
           institute=_DEFAULT_INSTITUTE):
    """Logs controlled vocabulary related events.

    :param msg: Message for writing to log.
    :type msg: str

    """
    log(msg, module="CV", level=level, app=app, institute=institute)


def log_cv_warning(
    msg,
    level=LOG_LEVEL_WARNING,
    app=_DEFAULT_APP,
    institute=_DEFAULT_INSTITUTE):
    """Logs controlled vocabulary warning related events.

    :param msg: Message for writing to log.
    :type msg: str

    """
    log(msg, module="CV", level=level, app=app, institute=institute)


def log_mq(msg,
           host=None,
           queue=None,
           level=LOG_LEVEL_INFO,
           app=_DEFAULT_APP,
           institute=_DEFAULT_INSTITUTE):
    """Logs message queue related event.

    :param host: MQ host name.
    :type host: str

    :param queue: MQ queue name.
    :type queue: str

    :param msg: Message for writing to log.
    :type msg: str

    """
    def _log(m):
        log(m, module="MQ", level=level, app=app, institute=institute)

    if host is None and queue is None:
        _log(msg)
    elif queue is None:
        _log("{0} :: {1}".format(msg, host))
    else:
        _log("{0} :: {1} :: {2}.".format(msg, host, queue))


def log_mq_error(err,
                 app=_DEFAULT_APP,
                 institute=_DEFAULT_INSTITUTE):
    """Logs a runtime error.

    :param err: Message for writing to log.
    :type err: Sub-class of BaseException

    """
    msg = "!!! MQ RUNTIME ERROR !!! :: {0} :: {1}.".format(err.__class__, err)
    log(msg, module="MQ", level=LOG_LEVEL_ERROR, app=app, institute=institute)


def log_db(msg,
           level=LOG_LEVEL_INFO,
           app=_DEFAULT_APP,
           institute=_DEFAULT_INSTITUTE):
    """Logs database related events.

    :param msg: Message for writing to log.
    :type msg: str

    """
    log(msg, module="DB", level=level, app=app, institute=institute)


def log_db_error(err, app=_DEFAULT_APP, institute=_DEFAULT_INSTITUTE):
    """Logs a runtime error.

    :param err: Message for writing to log.
    :type err: Sub-class of BaseException

    """
    if type(err) == str:
        msg = "!!! RUNTIME ERROR !!! :: {0}".format(err)
    else:
        msg = "!!! RUNTIME ERROR !!! :: {0} :: {1}.".format(err.__class__, err)
    log(msg, module="DB", level=LOG_LEVEL_ERROR, app=app, institute=institute)


def log_web(msg,
            level=LOG_LEVEL_INFO,
            app=_DEFAULT_APP,
            institute=_DEFAULT_INSTITUTE):
    """Logs api related events.

    :param msg: Message for writing to log.
    :type msg: str

    """
    log(msg, module="API", level=level, app=app, institute=institute)


def log_web_warning(
    msg,
    level=LOG_LEVEL_WARNING,
    app=_DEFAULT_APP,
    institute=_DEFAULT_INSTITUTE):
    """Logs api warning events.

    :param msg: Message for writing to log.
    :type msg: str

    """
    log(msg, module="API", level=level, app=app, institute=institute)


def log_web_error(err,
                  app=_DEFAULT_APP,
                  institute=_DEFAULT_INSTITUTE):
    """Logs a runtime error.

    :param err: Message for writing to log.
    :type err: Sub-class of BaseException

    """
    if type(err) == str:
        msg = "!!! RUNTIME ERROR !!! :: {0}".format(err)
    else:
        msg = "!!! RUNTIME ERROR !!! :: {0} :: {1}.".format(err.__class__, err)
    log(msg, module="API", level=LOG_LEVEL_ERROR, app=app, institute=institute)
