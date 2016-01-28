# -*- coding: utf-8 -*-

"""
.. module:: internal_alert.py
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

# Set of supported alert triggers.
_TRIGGERS = {
    _TRIGGER_SMTP_CHECKER_COUNT,
    _TRIGGER_SMTP_CHECKER_LATENCY
}

# Target email address to which emails will be sent.
_EMAIL_ADDRESSES = [i.strip() for i in config.operator.emailAddress.split(",")]

# Operator email subject template.
_EMAIL_SUBJECT = u"PRODIGUER OPS :: WARNING :: {}"

# Map of email subjects to trigger types.
_EMAIL_SUBJECT_MAP = {
    _TRIGGER_SMTP_CHECKER_COUNT: u"Too many unprocessed emails",
    _TRIGGER_SMTP_CHECKER_LATENCY: u"Emails taking too long to arrive from HPC"
}

# Operator email body template.
_EMAIL_BODY = u"""Dear Prodiguer platform operator,

A platform fault has occurrred which may require your attention.

{}.

Regards,

The Prodiguer Platform"""

# Map of email body text to trigger types.
_EMAIL_BODY_MAP = {
    _TRIGGER_SMTP_CHECKER_COUNT: u"The count of unprocessed emails ({}) exceeds the configured limit {}.",
    _TRIGGER_SMTP_CHECKER_LATENCY: u"The arrival latency of emails is excessive.  There may be an issue with the SMTP server(s)."
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
    if ctx.trigger not in [
        _TRIGGER_SMTP_CHECKER_COUNT,
        _TRIGGER_SMTP_CHECKER_LATENCY
        ]:
        return

    # Initialise email content.
    subject = _EMAIL_SUBJECT.format(_EMAIL_SUBJECT_MAP[ctx.trigger])
    body = _EMAIL_BODY.format(_EMAIL_BODY_MAP[ctx.trigger])

    # Enhance email content (when appropriate).
    if ctx.trigger == _TRIGGER_SMTP_CHECKER_COUNT:
        body = body.format(ctx.content.get('unprocessed_email_count'),
                           ctx.content.get('unprocessed_email_limit'))

    # Send email.
    mail.send_email(_EMAIL_ADDRESSES, subject, body)
