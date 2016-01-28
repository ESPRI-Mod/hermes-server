# -*- coding: utf-8 -*-

"""
.. module:: ext_smtp_checker.py
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
    # Retrieve all emails that have been queued for processing since last check.
    arrival_date = datetime.datetime.now() - datetime.timedelta(seconds=_RETRY_DELAY)
    with db.session.create():
        emails = db.dao_mq.retrieve_message_emails(arrival_date)
    if not emails:
        return

    # TODO algorithm to detect latency issues
    # for email in emails:
    #     print "AA", email.arrival_date, email.dispatch_date, email.dispatch_latency


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
        _log("checking smtp state ...")
        _do(_check_email_count)
        _do(_check_email_latency)
        time.sleep(_RETRY_DELAY)
