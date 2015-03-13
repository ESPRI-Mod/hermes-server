# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.utils.email.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Email utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import email, imaplib, imapclient

from prodiguer.utils.config import data as config



# IMAP config.
_CONFIG = config.mail.imap

# Email format identifier.
_RFC822 = u'RFC822'

# Email id header filter.
_MESSAGE_ID_HEADER = u'BODY[HEADER.FIELDS (MESSAGE-ID)]'

# Email search filter.
_SEARCH_FILTER_UNDELETED = ['NOT DELETED']

# Size of chunks used to split email downloads.
_CHUNK_SIZE = 50


def connect():
    """Connects to IMAP server and returns client.

    """
    # Connect to server.
    client = imapclient.IMAPClient(_CONFIG.host, use_uid=True, ssl=True)

    # Login.
    client.login(_CONFIG.username, _CONFIG.password)

    # Select folder.
    client.select_folder(_CONFIG.mailbox)

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
        for uid in chunk.keys():
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
    if mail.is_multipart():
        mail, attachment = mail.get_payload()
    else:
        attachment = None

    return mail, attachment


def move_email(email_uid, folder=None, client=None):
    """Moves an email to new mail box.

    :param str email_uid: Unique email identifier.
    :param str folder: Destination mailbox to which email will be moved.
    :param imapclient.IMAPClient client: An imap server client.

    """
    # Set imap proxy.
    proxy = client or connect()

    # Set folder.
    if not folder:
        folder = _CONFIG.mailbox_processed

    # Copy to new folder.
    proxy.copy(email_uid, folder)

    # Delete from old folder.
    proxy.delete_messages(email_uid)
    proxy.expunge()

    # Close imap proxy (if necessary).
    if not client:
        disconnect(proxy)
