# -*- coding: utf-8 -*-

"""
.. module:: ext_smtp_checker.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Checks SMTP server status.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import time

from prodiguer import __version__ as PRODIGUER_VERSION
from prodiguer import config
from prodiguer import mail
from prodiguer import mq
from prodiguer.utils import logger
from prodiguer_jobs.mq import utils as mq_utils



# Maximum number of emails allowed to reside in mailbox before triggering operator warning.
_MAX_UNPROCESSED = config.mq.mail.checker.maxUnprocessedCount

# Delay in seconds between checks.
_RETRY_DELAY = config.mq.mail.checker.retryDelayInSeconds


def _log(msg, level=logger.LOG_LEVEL_INFO):
    """Helper function: logs a message.

    """
    msg = "EXT-SMTP-CHECKER :: {}".format(msg)
    if level == logger.LOG_LEVEL_ERROR:
        logger.log_mq_error(msg)
    else:
        logger.log_mq(msg, level=level)


def _check_email_count():
    """Verifies that number of emails awaiting processing does not exceed a configurable limit.

    """
    _log("checking unprocessed email count ...")

    # Escape if number of unprocessed email is less than max allowed.
    emails = mail.get_email_uid_list()
    if len(emails) < _MAX_UNPROCESSED:
        return

    # Log.
    msg = "unprocessed email count {} exceeds limit {}."
    msg = msg.format(len(emails), _MAX_UNPROCESSED)
    _log(msg, logger.LOG_LEVEL_WARNING)

    # Alert operator.
    mq_utils.enqueue(mq.constants.MESSAGE_TYPE_ALERT, {
        "trigger": u"smtp-checker-count",
        "unprocessed_email_count": len(emails),
        "unprocessed_email_limit": _MAX_UNPROCESSED
        })


def _check_email_latency():
    """Verifies that average email arrival latency does not exceed a configurable period.

    """
    _log("checking received email latency")


def _do(func):
    """Runs a check.

    """
    try:
        func()
    except Exception as err:
        _log(err, logger.LOG_LEVEL_ERROR)


def execute(throttle=0):
    """Executes realtime SMTP sourced message production.

    """
    while True:
        time.sleep(_RETRY_DELAY)
        _do(_check_email_count)
        _do(_check_email_latency)
