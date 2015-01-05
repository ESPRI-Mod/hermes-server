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

    for func in (
        client.idle_done,
        client.close_folder,
        client.logout
        ):
        try:
            func()
        except imaplib.IMAP4.abort:
            pass


def get_email_uid_list(client=None):
    """Returns emails for processing.

    :param imapclient.IMAPClient client: An imap server client.

    :returns: List of emails for processing.
    :rtype: list

    """
    # Set imap client.
    is_new_client = client is None
    if is_new_client:
        client = connect()

    # Get emails of interest.
    targets = client.search(_SEARCH_FILTER_UNDELETED)

    # Get emails message identifiers.
    targets = client.fetch(targets, _MESSAGE_ID_HEADER)

    # Close imap client (if necessary).
    if is_new_client:
        disconnect(client)

    # Set de-duplicated email uid map.
    uid_map = {}
    for uid in targets.keys():
        identifier = targets[uid][_MESSAGE_ID_HEADER]
        if identifier not in uid_map:
            uid_map[identifier] = uid

    return uid_map.values()


def get_email(email_uid, client=None):
    """Returns an email plus attachment from IMAP server.

    :param str email_uid: Unique email identifier.
    :param imapclient.IMAPClient client: An imap server client.

    :returns: 2 member tuple of mail body and attachment.
    :rtype: tuple

    """
    # Set imap client.
    is_new_client = client is None
    if is_new_client:
        client = connect()

    # Fetch email.
    data = client.fetch(email_uid, _RFC822)

    # Close imap client (if necessary).
    if is_new_client:
        disconnect(client)

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
    # Set imap client.
    is_new_client = client is None
    if is_new_client:
        client = connect()

    # Set folder.
    if not folder:
        folder = _CONFIG.mailbox_processed

    # Copy to new folder.
    client.copy(email_uid, folder)

    # Delete from old folder.
    client.delete_messages(email_uid)
    client.expunge()

    # Close imap client (if necessary).
    if is_new_client:
        disconnect(client)
