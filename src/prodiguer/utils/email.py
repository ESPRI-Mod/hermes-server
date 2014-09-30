# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.utils.email.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Email utility functions.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import imaplib, imapclient

from config import data as config



# IMAP config.
_CONFIG = config.mail.imap


def get_imap_proxy():
    """Returns IMAP server proxy."""
    # Connect to server.
    proxy = imapclient.IMAPClient(_CONFIG.host, use_uid=True, ssl=True)

    # Login.
    proxy.login(_CONFIG.username, _CONFIG.password)

    # Select folder.
    proxy.select_folder(_CONFIG.mailbox)

    return proxy


def close_imap_proxy(proxy):
    """Closes imap server proxy."""
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
    """Returns emails for processing."""
    if not proxy:
        proxy = get_imap_proxy()

    return proxy.search(['NOT DELETED'])
