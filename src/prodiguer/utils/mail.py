# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.utils.email.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Email utility functions.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import email, imaplib, imapclient

import config




# IMAP config.
_CONFIG = config.data.mail.imap

# Email format identifier.
_RFC822 = u'RFC822'


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

    return proxy.search(['NOT DELETED'])


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
    mail = email.message_from_string(data[email_uid][_RFC822])
    if mail.is_multipart():
        mail, attachment = mail.get_payload()
    else:
        attachment = None

    return mail, attachment
