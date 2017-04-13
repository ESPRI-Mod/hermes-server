# -*- coding: utf-8 -*-

"""
.. module:: pyesdoc.utils.security.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of security related utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os

from hermes.utils import logger
from hermes.utils.convert import dict_to_namedtuple
from hermes.utils.convert import json_to_namedtuple



# No-op security configuration data.
_DEFAULT_CONFIG = {
    'users': []
}


def _get_configuration():
    """Load configuration file content from file system.

    """
    path = os.getenv("HERMES_HOME")
    path = os.path.join(path, "ops")
    path = os.path.join(path, "config")
    path = os.path.join(path, "hermes-users.json")

    # Return default if not found.
    if not os.path.exists(path):
        msg = "HERMES user configuration file does not exist :: {}"
        logger.log_mq_warning(msg.format(path))
        return dict_to_namedtuple(_DEFAULT_CONFIG)

    with open(path, 'r') as data:
        return json_to_namedtuple(data.read())


# Configuration data.
DATA = _get_configuration()

# Set of registered users.
USERS = DATA.users


def get_user(login):
    """Returns a user matched by login (case insensitve).

    """
    login = unicode(login).lower()
    for i in USERS:
        if unicode(i.login).lower() == login:
            return i
