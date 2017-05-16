# -*- coding: utf-8 -*-

"""
.. module:: hermes.utils.email.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Email utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import email
import imaplib
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formatdate

import arrow
import imapclient

from hermes.utils.config import data as config



# Email server.
SERVER = None

# Email format identifier.
_RFC822 = u'RFC822'

# Email id header filter.
_MESSAGE_ID_HEADER = u'BODY[HEADER.FIELDS (MESSAGE-ID)]'

# Email search filter.
_SEARCH_FILTER_UNDELETED = 'NOT DELETED'

# Size of chunks used to split email downloads.
_CHUNK_SIZE = 50

# Email date format.
_DATE_FORMAT = "ddd, DD MMM YYYY HH:mm:ss ZZ"
_DATE_FORMATS = [
    "ddd, D MMM YYYY HH:mm:ss ZZ",
    "ddd, DD MMM YYYY HH:mm:ss ZZ"
]


def set_server(server_id=1):
    """Iniitalises module.

    :param str server_id: ID of mail server to point to.

    """
    global SERVER

    try:
        SERVER = config.mq.emailServers[int(server_id) - 1]
    except (ValueError, IndexError):
        raise ValueError("Invalid email server ID [{}]".format(server_id))


def connect():
    """Connects to IMAP server and returns client.

    """
    # Ensure server is assigned.
    if SERVER is None:
        set_server()

    # Connect to server.
    client = imapclient.IMAPClient(SERVER.host, use_uid=True, ssl=True)

    # Login.
    client.login(SERVER.username, SERVER.password)

    # Select folder.
    client.select_folder(SERVER.mailbox)

    return client


def disconnect(client):
    """Closes imap server client.

    :param imapclient.IMAPClient client: An imap server client.

    """
    if not client:
        return

    try:
        client.idle_done()
    except imaplib.IMAP4.abort:
        pass
    except AttributeError:
        pass

    try:
        client.close_folder()
    except imaplib.IMAP4.abort:
        pass

    try:
        client.logout()
    except imaplib.IMAP4.abort:
        pass


def reconnect(client):
    """Closes existing imap server client and returns a new instance.

    :param imapclient.IMAPClient client: An imap server client.

    """
    disconnect(client)

    return connect()


def get_email_uid_list(client=None):
    """Returns emails for processing.

    :param imapclient.IMAPClient client: An imap server client.

    :returns: List of emails for processing.
    :rtype: list

    """
    def _chunkify(uids):
        """Returns chunks of email uid's to be processed.

        """
        return [uids[i:i + _CHUNK_SIZE] for i in \
                range(0, len(uids), _CHUNK_SIZE)]

    # Set imap proxy.
    proxy = client or connect()

    # Clear items marked for deletion.
    proxy.expunge()

    # Set chunks of emails to be downloaded.
    chunks = [proxy.fetch(c, _MESSAGE_ID_HEADER) for c in \
              _chunkify(proxy.search(_SEARCH_FILTER_UNDELETED))]

    # Close imap proxy (if necessary).
    if not client:
        disconnect(proxy)

    # Set de-duplicated email uid map.
    uid_map = {}
    for chunk in chunks:
        for uid in chunk:
            try:
                header = chunk[uid][_MESSAGE_ID_HEADER]
            except KeyError:
                pass
            else:
                if header not in uid_map:
                    uid_map[header] = uid

    return sorted(uid_map.values())


def get_email(email_uid, client=None):
    """Returns an email plus attachment from IMAP server.

    :param str email_uid: Unique email identifier.
    :param imapclient.IMAPClient client: An imap server client.

    :returns: 2 member tuple of mail body and attachment.
    :rtype: tuple

    """
    # Set imap proxy.
    proxy = client or connect()

    # Fetch email.
    data = proxy.fetch(email_uid, _RFC822)

    # Close imap proxy (if necessary).
    if not client:
        disconnect(proxy)

    # Validate imap response.
    if email_uid not in data:
       raise ValueError("WARNING :: Email {0} not found.".format(email_uid))
    if _RFC822 not in data[email_uid]:
       raise ValueError("WARNING :: Email {0} content empty.".format(email_uid))

    # Unpack email.
    mail = data[email_uid][_RFC822]
    mail = email.message_from_string(mail)

    # Unpack attachments.
    attachments = []
    if mail.is_multipart():
        payload = mail.get_payload()
        mail = payload[0]
        attachments = payload[1:]

    return mail, attachments


def delete_email(email_uid, client=None):
    """Deletes an email.

    :param str email_uid: Identifier of email to be deleted.
    :param str folder: Destination mailbox to which email will be moved.
    :param imapclient.IMAPClient client: An imap server client.

    """
    # Set imap proxy.
    proxy = client or connect()

    # Delete from old folder.
    proxy.delete_messages(email_uid)
    proxy.expunge()

    # Close imap proxy (if necessary).
    if not client:
        disconnect(proxy)


def move_email(email_uid, folder=None, client=None):
    """Moves an email to new mail box.

    :param str email_uid: Unique email identifier.
    :param str folder: Destination mailbox to which email will be moved.
    :param imapclient.IMAPClient client: An imap server client.

    """
    # Ensure server is assigned.
    if SERVER is None:
        set_server()

    # Set imap proxy.
    proxy = client or connect()

    # Set folder.
    if not folder:
        folder = SERVER.mailbox_processed

    # Copy to new folder.
    proxy.copy(email_uid, folder)

    # Delete from old folder.
    delete_email(email_uid, proxy)

    # Close imap proxy (if necessary).
    if not client:
        disconnect(proxy)


def get_email_arrival_date(body):
    """Returns email arrival date.

    """
    def _get_header():
        """Returns email arrival header.

        """
        targets = []
        for key, val in body.items():
            if key.lower() == 'received':
                targets.append(val)
        targets.reverse()

        return targets[-1]


    def _get_date(val):
        """Returns email arrival date.

        """
        val = val.split(";")[-1].split("(")[0].strip()
        if val.find("\n") != -1:
            x = val.split("\n")[0].strip()
            y = val.split("\n")[1].strip()
            val = "{} {}".format(x, y)

        for date_format in _DATE_FORMATS:
            try:
                return arrow.get(val, date_format)
            except Exception as err:
                if date_format == _DATE_FORMATS[-1]:
                    raise err
                pass


    return _get_date(_get_header())


def get_email_dispatch_date(body):
    """Returns email dispatch date.

    :param dict body: Email body.

    :return: Date of emil dispatch.
    :rtype: arrow.date

    """
    for date_format in _DATE_FORMATS:
        try:
            return arrow.get(body['Date'], date_format)
        except Exception as err:
            if date_format == _DATE_FORMATS[-1]:
                raise err
            pass


def send_email(
    address_from,
    address_to,
    subject,
    body,
    attachment=None,
    attachment_name=None
    ):
    """Dispatches email to Hermes email server.

    :param str address_from: Email address to use as sender's address.
    :param str address_to: Email address to which email will be delivered.
    :param str subject: Subject of email to be dispatched.
    :param str body: Body of email to be dispatched.
    :param str attachment: Email attachment.
    :param str attachment_name: Name to be associated with the email attachment.

    """
    # Ensure server is assigned.
    if SERVER is None:
        set_server()

    # Initalise email.
    msg = MIMEMultipart()
    msg['Subject'] = subject
    msg['From'] = address_from
    msg['To'] = address_to
    msg['Date'] = formatdate(localtime=True)

    # Set email body.
    msg.attach(MIMEText(body + "\n\n\n"))

    # Append attachment.
    if attachment:
        attachment_name = attachment_name or "unknown"
        msg.attach(MIMEApplication(attachment, Name=attachment_name))

    # Connect to mail server.
    mailserver = smtplib.SMTP(SERVER.host, port=SERVER.smtpPort)
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.ehlo()
    mailserver.login(SERVER.username, SERVER.password)

    # Dispatch email.
    mailserver.sendmail(address_from, address_to, msg.as_string())
