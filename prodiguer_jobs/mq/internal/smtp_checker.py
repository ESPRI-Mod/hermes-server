# -*- coding: utf-8 -*-

"""
.. module:: smtp_checker.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Checks SMTP server status.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime
import time

from prodiguer import config
from prodiguer import mail
from prodiguer import mq
from prodiguer.db import pgres as db
from prodiguer.utils import logger
from prodiguer_jobs.mq.utils import enqueue



# Mail server config.
_CONFIG = config.mq.mail.checker

# Maximum number of emails allowed to reside in mailbox before triggering operator warning.
_MAX_UNPROCESSED = _CONFIG.maxUnprocessedCount

# Maximum number of seconds between email dispatch & email arrival before triggering operator warning.
_MAX_LATENCY = _CONFIG.maxLatencyInSeconds

# Delay in seconds between checks.
_RETRY_DELAY = _CONFIG.retryDelayInSeconds


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
    # Escape if number of unprocessed email is less than max allowed.
    emails = mail.get_email_uid_list()
    if len(emails) < _MAX_UNPROCESSED:
        return

    # Log.
    msg = "unprocessed email count {} exceeds limit {}."
    msg = msg.format(len(emails), _MAX_UNPROCESSED)
    _log(msg, logger.LOG_LEVEL_WARNING)

    # Alert operator.
    enqueue(mq.constants.MESSAGE_TYPE_ALERT, {
        "trigger": u"smtp-checker-count",
        "unprocessed_email_count": len(emails),
        "unprocessed_email_limit": _MAX_UNPROCESSED
        })


def _check_email_latency():
    """Verifies that average email arrival latency does not exceed a configurable period.

    """
    # TODO :: switched off pending further testing.
    return

    # Retrieve all emails that have been queued for processing since last check.
    arrival_date = datetime.datetime.now() - datetime.timedelta(seconds=_RETRY_DELAY)
    with db.session.create():
        emails = db.dao_mq.retrieve_message_emails(arrival_date)
    if not emails:
        return

    # Set late emails.
    late = []
    if not late:
        return

    # Log.
    msg = "{} emails have latencies that exceed the limit {} seconds."
    msg = msg.format(len(late), _MAX_LATENCY)
    _log(msg, logger.LOG_LEVEL_WARNING)

    # Alert operator.
    enqueue(mq.constants.MESSAGE_TYPE_ALERT, {
        "trigger": u"smtp-checker-latency",
        "max_latency": _MAX_LATENCY,
        "number_of_late_emails": len(late)
        })


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
        _log("checking SMTP server ...")
        _do(_check_email_count)
        _do(_check_email_latency)
