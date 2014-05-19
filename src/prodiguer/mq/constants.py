# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.constants.py
   :platform: Unix
   :synopsis: Prodiguer mq constants.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# AMPQ message exchange types.
AMPQ_EXCHANGE_TYPE_DIRECT = "direct"
AMPQ_EXCHANGE_TYPE_FANOUT = "fanout"
AMPQ_EXCHANGE_TYPE_HEADER = "header"
AMPQ_EXCHANGE_TYPE_TOPIC = "topic"

# All AMPQ exchange types.
AMPQ_EXCHANGE_TYPES = set([
	AMPQ_EXCHANGE_TYPE_DIRECT,
	AMPQ_EXCHANGE_TYPE_FANOUT,
	AMPQ_EXCHANGE_TYPE_HEADER,
	AMPQ_EXCHANGE_TYPE_TOPIC
	])

# AMPQ message delivery modes.
AMPQ_DELIVERY_MODE_NON_PERSISTENT = 1
AMPQ_DELIVERY_MODE_PERSISTENT = 2

# All AMPQ message delivery modes.
AMPQ_DELIVERY_MODES = set([
	AMPQ_DELIVERY_MODE_NON_PERSISTENT,
	AMPQ_DELIVERY_MODE_PERSISTENT
	])

# Message exchanges.
EXCHANGE_PRODIGUER = "ipsl-prodiguer"
EXCHANGE_PRODIGUER_INTERNAL = "ipsl-prodiguer-internal"

# All exchanges.
EXCHANGES = set([
	EXCHANGE_PRODIGUER,
	EXCHANGE_PRODIGUER_INTERNAL
	])

# Message producers.
PRODUCER_IGCM = "liblIGCM"

# All producers.
PRODUCERS = set([
	PRODUCER_IGCM
	])

# Message application identifiers.
APP_SMON = "sim-mon"

# All apps.
APPS = set([
	APP_SMON,
	])

# Message types:
# ... simulation monitoring.
TYPE_SMON_0000 = "0000"
TYPE_SMON_1000 = "1000"
TYPE_SMON_2000 = "2000"
TYPE_SMON_3000 = "3000"
TYPE_SMON_9000 = "9000"
TYPE_SMON_9999 = "9999"

# All types.
TYPES = set([
	TYPE_SMON_0000,
	TYPE_SMON_1000,
	TYPE_SMON_2000,
	TYPE_SMON_3000,
	TYPE_SMON_9000,
	TYPE_SMON_9999,
	])

# Message modes.
MODE_DEV = 'dev'
MODE_TEST = 'test'
MODE_PROD = 'prod'

# All modes.
MODES = set([
	MODE_DEV,
	MODE_TEST,
	MODE_PROD,
	])

# Content types.
CONTENT_TYPE_JSON = 'application/json'
CONTENT_TYPE_BASE64 = 'application/base64'

# All content types.
CONTENT_TYPES = set([
	CONTENT_TYPE_JSON,
	CONTENT_TYPE_BASE64,
	])

# Content encodings.
CONTENT_ENCODING_UNICODE = "utf-8"

# All content encodings.
CONTENT_ENCODINGS = set([
	CONTENT_ENCODING_UNICODE
	])

# Message priorities.
PRIORITY_LOW = 1
PRIORITY_NORMAL = 4
PRIORITY_HIGH = 7
PRIORITY_URGENT = 9
PRIORITY_CRITICAL = 10

# All message priorities.
PRIORITIES = set([
	PRIORITY_LOW,
	PRIORITY_NORMAL,
	PRIORITY_HIGH,
	PRIORITY_URGENT,
	PRIORITY_CRITICAL,
	])

# Default application.
DEFAULT_APP = APP_SMON

# Default message expiration.
DEFAULT_EXPIRATION = None

# Default system mode.
DEFAULT_MODE = MODE_TEST

# Default message exchange.
DEFAULT_EXCHANGE = EXCHANGE_PRODIGUER

# Default message exchange type.
DEFAULT_EXCHANGE_TYPE = AMPQ_EXCHANGE_TYPE_TOPIC

# Default publishing limit (used in automated tests).
DEFAULT_PUBLISH_LIMIT = 1

# Default publishing interval (used in automated tests).
DEFAULT_PUBLISH_INTERVAL = 0

# Default publishing frequency (used in automated tests).
DEFAULT_PUBLISH_FREQUENCY = 1

# Default message priority.
DEFAULT_PRIORITY = PRIORITY_NORMAL

# Default message producer.
DEFAULT_PRODUCER = PRODUCER_IGCM

# Default user.
DEFAULT_USER = "guest"

# Time (in seconds) before a connection retry is attempted.
DEFAULT_CONNECTION_REOPEN_DELAY = 5

