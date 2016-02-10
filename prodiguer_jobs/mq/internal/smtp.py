# -*- coding: utf-8 -*-

"""
.. module:: smtp.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Enqueues messages embedded in enqueued emails received from libIGCM.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import base64
import copy
import json
import uuid

from prodiguer import config
from prodiguer import mail
from prodiguer import mq
from prodiguer.db import pgres as db
from prodiguer.utils import logger



# Mail server config.
_CONFIG = config.mq


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _set_email,
        _persist_email_delivery_stats,
        _set_msg_b64,
        _set_msg_json,
        _set_msg_dict,
        _drop_excluded_messages,
        _process_attachments,
        _set_msg_ampq,
        _enqueue_messages,
        _log_stats,
        _persist_stats,
        _dequeue_email,
        _close_imap_client
        )


def get_error_tasks():
    """Returns set of tasks to be executed when a message processing error occurs.

    """
    return _close_imap_client


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(props, body, decode=True)

        self.email = None
        self.email_attachments = None
        self.email_body = None
        self.email_uid = self.content['email_uid']
        self.imap_client = None
        self.msg_ampq = []
        self.msg_ampq_error = []
        self.msg_b64 = []
        self.msg_json = []
        self.msg_json_error = []
        self.msg_dict = []
        self.msg_dict_error = []
        self.msg_dict_excluded = []


def _set_email(ctx):
    """Set email to be processed.

    """
    # Connect to imap server.
    ctx.imap_client = mail.connect()

    # Download & decode email.
    body, attachments = mail.get_email(ctx.email_uid, ctx.imap_client)
    ctx.email = body.get_payload(decode=True)
    ctx.email_body = body
    ctx.email_attachments = [a.get_payload(decode=True) for a in attachments]


def _persist_email_delivery_stats(ctx):
    """Persists email delivery statistical information to database.

    """
    def _get_date(func):
        """Returns a date field from the email headers.

        """
        # N.B. override errors as email headers can be inconsistent.
        try:
            return func(ctx.email_body).datetime
        except:
            return None

    # Update message email table.
    db.dao_mq.update_message_email(
        ctx.email_uid,
        _get_date(mail.get_email_arrival_date),
        _get_date(mail.get_email_dispatch_date)
        )


def _set_msg_b64(ctx):
    """Sets base64 encoded messages to be processed.

    """
    ctx.msg_b64 += [l for l in ctx.email.splitlines() if l]


def _set_msg_json(ctx):
    """Set json encoded messages to be processed.

    """
    for msg in ctx.msg_b64:
        try:
            ctx.msg_json.append(base64.b64decode(msg))
        except:
            ctx.msg_json_error.append(msg)


def _set_msg_dict(ctx):
    """Set dictionary encoded messages to be processed.

    """
    for msg in ctx.msg_json:
        try:
            ctx.msg_dict.append(json.loads(msg))
        except:
            try:
                msg = msg.replace('\\', '')
                ctx.msg_dict.append(json.loads(msg))
            except:
                ctx.msg_dict_error.append(msg)


def _drop_excluded_messages(ctx):
    """Drops messages that are excluded due to their type.

    """
    def _is_excluded(msg):
        """Determines whether the message is deemed to be excluded.

        """
        return 'msgProducerVersion' not in msg or \
               msg['msgCode'] in _CONFIG.excludedTypes

    ctx.msg_dict_excluded = [m for m in ctx.msg_dict if _is_excluded(m)]
    ctx.msg_dict = [m for m in ctx.msg_dict if m not in ctx.msg_dict_excluded]


def _process_attachments_0000(ctx):
    """Processes email attachments for message type 0000.

    """
    msg = ctx.msg_dict[0]
    msg['configuration'] = ctx.email_attachments[0]


def _process_attachments_7100(ctx):
    """Processes email attachments for message type 7100.

    """
    msg = ctx.msg_dict[0]
    ctx.msg_dict = []
    for attachment in ctx.email_attachments:
        new_msg = copy.deepcopy(msg)
        new_msg['msgUID'] = unicode(uuid.uuid4())
        new_msg['metrics'] = base64.encodestring(attachment)
        ctx.msg_dict.append(new_msg)


# Map of attachment handlers to message types.
_ATTACHMENT_HANDLERS = {
    '0000': _process_attachments_0000,
    '7100': _process_attachments_7100
}


def _process_attachments(ctx):
    """Processes an email attchment.

    """
    # Escape if there are no attachments to process.
    if not ctx.email_attachments:
        return

    # Escape if attachment is not associated with a single message.
    if len(ctx.msg_dict) != 1:
        return

    # Escape if message type is not mapped to a handler.
    msg = ctx.msg_dict[0]
    msg_code = msg['msgCode']
    if msg_code not in _ATTACHMENT_HANDLERS:
        return

    # Invoke handler.
    handler = _ATTACHMENT_HANDLERS[msg_code]
    handler(ctx)


def _set_msg_ampq(ctx):
    """Set AMPQ messages to be dispatched.

    """
    def _get_ampq_props(data):
        """Returns an AMPQ basic properties instance, i.e. message header.

        """
        # Decode nano-second precise timestamp.
        timestamp = mq.Timestamp.from_ns(data['msgTimestamp'])

        return mq.utils.create_ampq_message_properties(
            user_id=mq.constants.USER_PRODIGUER,
            producer_id=data['msgProducer'],
            producer_version=data['msgProducerVersion'],
            message_id=data['msgUID'],
            message_type=data['msgCode'],
            timestamp=timestamp.as_ms_int,
            headers={
                'timestamp': unicode(timestamp.as_ns_raw),
                'timestamp_precision': u'ns',
                'correlation_id_1': data.get('simuid'),
                'correlation_id_2': data.get('jobuid'),
                'email_id': ctx.email_uid
            })


    def _get_ampq_payload(data):
        """Return ampq message payload.

        """
        # Strip out non-platform platform attributes.
        return { k: data[k] for k in data.keys() if not k.startswith("msg") }


    def _encode(data):
        """Encodes data as an ampq message.

        """
        try:
            return mq.Message(_get_ampq_props(data), _get_ampq_payload(data))
        except Exception as err:
            return data, err


    for msg in [_encode(m) for m in ctx.msg_dict]:
        if isinstance(msg, tuple):
            ctx.msg_ampq_error.append(msg)
        else:
            ctx.msg_ampq.append(msg)


def _enqueue_messages(ctx):
    """Enqueues messages upon MQ server.

    """
    mq.produce(ctx.msg_ampq, connection_url=config.mq.connections.main)


def _dequeue_email(ctx):
    """Removes email from mailbox after processing.

    """
    if _CONFIG.mail.deleteProcessed:
        mail.delete_email(ctx.email_uid, client=ctx.imap_client)
    else:
        mail.move_email(ctx.email_uid, client=ctx.imap_client)


def _close_imap_client(ctx):
    """Closes imap client after use.

    """
    try:
        mail.disconnect(ctx.imap_client)
    except:
        logger.log_mq_warning("IMAP server disconnection error, error was discarded.")


def _log_stats(ctx):
    """Logs processing statistics.

    """
    msg = "Email uid: {};  ".format(ctx.email_uid)
    msg += "Incoming: {};  ".format(len(ctx.msg_b64))
    if ctx.msg_json_error:
        msg += "Base64 decoding errors: {};  ".format(len(ctx.msg_json_error))
    if ctx.msg_dict_error:
        msg += "JSON encoding errors: {};  ".format(len(ctx.msg_dict_error))
    if ctx.msg_ampq_error:
        msg += "AMPQ encoding errors: {};  ".format(len(ctx.msg_ampq_error))
    if ctx.msg_dict_excluded:
        msg += "Type Exclusions: {};  ".format(len(ctx.msg_dict_excluded))
    msg += "Outgoing: {}.".format(len(ctx.msg_ampq))

    logger.log_mq(msg)


def _persist_stats(ctx):
    """Persists processing statistics.

    """
    def _get_outgoing_message_count(type_id):
        """Returns count of messages dispatched to MQ server.

        """
        return len([m for m in ctx.msg_ampq if m.props.type == type_id])


    db.dao_mq.persist_message_email_stats(
        ctx.email_uid,
        incoming=len(ctx.msg_b64),
        errors_decoding_base64=len(ctx.msg_json_error),
        errors_decoding_json=len(ctx.msg_dict_error),
        errors_encoding_ampq=len(ctx.msg_ampq_error),
        excluded=len(ctx.msg_dict_excluded),
        outgoing=len(ctx.msg_ampq),
        outgoing_0000=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_0000),
        outgoing_0100=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_0100),
        outgoing_1000=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_1000),
        outgoing_1100=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_1100),
        outgoing_1900=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_1900),
        outgoing_1999=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_1999),
        outgoing_2000=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_2000),
        outgoing_2100=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_2100),
        outgoing_2900=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_2900),
        outgoing_2999=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_2999),
        outgoing_3000=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_3000),
        outgoing_3100=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_3100),
        outgoing_3900=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_3900),
        outgoing_3999=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_3999),
        outgoing_7000=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_7000),
        outgoing_7010=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_7010),
        outgoing_7100=_get_outgoing_message_count(mq.constants.MESSAGE_TYPE_7100)
        )
