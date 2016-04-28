# -*- coding: utf-8 -*-

"""
.. module:: alert.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Dispatches platform alerts via email/sms ... etc.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from prodiguer.utils import config
from prodiguer.utils import mail
from prodiguer.utils import logger



# Alert trigger: SMTP unprocessed count > limit.
_TRIGGER_SMTP_CHECKER_COUNT = u"smtp-checker-count"

# Alert trigger: SMTP email latency > time period.
_TRIGGER_SMTP_CHECKER_LATENCY = u"smtp-checker-latency"

# Alert trigger: Conso inactive allocation.
_TRIGGER_CONSO_INACTIVE_ALLOCATION = u"conso-inactive-allocation"

# Alert trigger: Conso new allocation.
_TRIGGER_CONSO_NEW_ALLOCATION = u"conso-new-allocation"

# Set of supported alert triggers.
_TRIGGERS = {
    _TRIGGER_SMTP_CHECKER_COUNT,
    _TRIGGER_SMTP_CHECKER_LATENCY,
    _TRIGGER_CONSO_INACTIVE_ALLOCATION,
    _TRIGGER_CONSO_NEW_ALLOCATION
}

# Operator email subject template.
_EMAIL_SUBJECT = u"PRODIGUER-OPS :: MQ PLATFORM :: WARNING :: {}"

# Operator email body template.
_EMAIL_BODY = u"""Dear Prodiguer platform operator,

{}

Regards,

The Prodiguer Platform"""

# Map of email content to triggers.
_EMAIL_MAP = {
    _TRIGGER_SMTP_CHECKER_COUNT: {
        "body": u"The count of unprocessed emails ({}) exceeds the configured limit ({}).  Either the smtp-realtime daemon is down or the platform is being restarted after a maintenance period.",
        "subject": u"Too many unprocessed emails"
    },
    _TRIGGER_SMTP_CHECKER_LATENCY: {
        "body": u"The arrival latency of emails is excessive.  There may be an issue with the SMTP server(s).",
        "subject": u"Emails taking too long to arrive from HPC"
    },
    _TRIGGER_CONSO_INACTIVE_ALLOCATION: {
        "body": u"{} CPT data was mapped to an inactive allocation [{}].",
        "subject": u"CONSO :: {} :: Allocation is inactive."
    },
    _TRIGGER_CONSO_NEW_ALLOCATION: {
        "body": u"{} CPT data could not be mapped to an existing allocation, a new allocation [{}] was therefore created.",
        "subject": u"CONSO :: {} :: New allocation."
    }
}


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _dispatch_operator_email
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.trigger = None


def _unpack_content(ctx):
    """Unpacks message content.

    """
    ctx.trigger = ctx.content.get('trigger')
    if ctx.trigger not in _TRIGGERS:
        logger.log_mq_warning("Alert trigger code ({}) is unsupported.".format(ctx.trigger))
        ctx.abort = True


def _dispatch_operator_email(ctx):
    """Dispatches an email to platform operator(s).

    """
    # Escape if trigger type out of scope.
    if ctx.trigger not in _EMAIL_MAP:
        return

    # Initialise email content.
    subject = _EMAIL_SUBJECT.format(_EMAIL_MAP[ctx.trigger]['subject'])
    body = _EMAIL_BODY.format(_EMAIL_MAP[ctx.trigger]['body'])

    # Enhance email content (when appropriate).
    if ctx.trigger == _TRIGGER_SMTP_CHECKER_COUNT:
        body = body.format(ctx.content.get('unprocessed_email_count'),
                           ctx.content.get('unprocessed_email_limit'))

    elif ctx.trigger == _TRIGGER_CONSO_INACTIVE_ALLOCATION:
        body = body.format(ctx.content.get('centre').upper(),
                           ctx.content.get('allocation_id'))
        subject = subject.format(ctx.content.get('centre').upper())

    elif ctx.trigger == _TRIGGER_CONSO_NEW_ALLOCATION:
        body = body.format(ctx.content.get('centre').upper(),
                           ctx.content.get('allocation_id'))
        subject = subject.format(ctx.content.get('centre').upper())

    # Send email.
    mail.send_email(config.alerts.emailAddressFrom,
                    config.alerts.emailAddressTo,
                    subject,
                    body)

    logger.log_mq("Email dispatched to operator :: trigger code = ({}).".format(ctx.trigger))
