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


def get_imap_proxy():
    """Returns IMAP server proxy instance.

    """
    # Connect to server.
    proxy = imapclient.IMAPClient(_CONFIG.host, use_uid=True, ssl=True)

    # Login.
    proxy.login(_CONFIG.username, _CONFIG.password)

    # Select folder.
    proxy.select_folder(_CONFIG.mailbox)

    return proxy


def close_imap_proxy(proxy):
    """Closes imap server proxy.

    :param imapclient.IMAPClient proxy: An imap proxy.

    """
    if proxy:
        for func in (
            proxy.idle_done,
            proxy.close_folder,
            proxy.logout
            ):
            try:
                func()
            except imaplib.IMAP4.abort:
                pass


def get_email_uid_list(proxy=None):
    """Returns emails for processing.

    :param imapclient.IMAPClient proxy: An imap proxy.

    :returns: List of emails for processing.
    :rtype: list

    """
    if not proxy:
        proxy = get_imap_proxy()

    # Get emails of interest.
    targets = proxy.search(['NOT DELETED'])

    # Get emails message identifiers.
    targets = proxy.fetch(targets, _MESSAGE_ID_HEADER)

    # Set de-duplicated email uid map.
    uid_map = {}
    for uid in targets.keys():
        identifier = targets[uid][_MESSAGE_ID_HEADER]
        if identifier not in uid_map:
            uid_map[identifier] = uid

    return uid_map.values()


def get_email(email_uid, proxy=None):
    """Returns an email plus attachment from IMAP server.

    :param str email_uid: Unique email identifier.
    :param imapclient.IMAPClient proxy: An imap proxy.

    :returns: 2 member tuple of mail body and attachment.
    :rtype: tuple

    """
    if not proxy:
        proxy = get_imap_proxy()

    # Fetch email.
    data = proxy.fetch(email_uid, _RFC822)

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


def move_email(email_uid, folder=None, proxy=None):
    """Moves an email to new mail box.

    :param str email_uid: Unique email identifier.
    :param str folder: Destination mailbox to which email will be moved.
    :param imapclient.IMAPClient proxy: An imap proxy.

    """
    if not proxy:
        proxy = get_imap_proxy()
    if not folder:
        folder = _CONFIG.mailbox_processed

    # Copy to new folder.
    proxy.copy(email_uid, folder)

    # Delete from old folder.
    proxy.delete_messages(email_uid)
    proxy.expunge()
