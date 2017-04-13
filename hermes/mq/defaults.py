# -*- coding: utf-8 -*-

"""
.. module:: hermes.mq.default.py
   :platform: Unix
   :synopsis: Hermes mq defaults.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from hermes.mq import constants



# Default delivery mode.
DEFAULT_DELIVERY_MODE = constants.AMPQ_DELIVERY_MODE_PERSISTENT

# Default content type.
DEFAULT_CONTENT_TYPE = constants.CONTENT_TYPE_JSON

# Default content encoding.
DEFAULT_CONTENT_ENCODING = constants.CONTENT_ENCODING_UNICODE

# Default message priority.
DEFAULT_PRIORITY = constants.PRIORITY_NORMAL

# Default publishing limit (used in automated tests).
DEFAULT_PUBLISH_LIMIT = 1

# Default publishing interval (used in automated tests).
DEFAULT_PUBLISH_INTERVAL = 0

# Default delay (in seconds) before an MQ server connection
# retry is attempted.
DEFAULT_CONNECTION_REOPEN_DELAY = 5
