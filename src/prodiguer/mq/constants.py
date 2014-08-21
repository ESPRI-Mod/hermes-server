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

# Default message exchange type.
DEFAULT_EXCHANGE_TYPE = AMPQ_EXCHANGE_TYPE_TOPIC

# AMPQ message delivery modes.
AMPQ_DELIVERY_MODE_NON_PERSISTENT = 1
AMPQ_DELIVERY_MODE_PERSISTENT = 2

# All AMPQ message delivery modes.
AMPQ_DELIVERY_MODES = set([
	AMPQ_DELIVERY_MODE_NON_PERSISTENT,
	AMPQ_DELIVERY_MODE_PERSISTENT
	])

# Default delivery mode.
DEFAULT_DELIVERY_MODE = AMPQ_DELIVERY_MODE_PERSISTENT

# Message server virtual host.
VHOST = "prodiguer"

# Message server exchanges.
EXCHANGE_PRODIGUER_EXT = "x-ext"
EXCHANGE_PRODIGUER_IN = "x-in"
EXCHANGE_PRODIGUER_INTERNAL = "x-internal"
EXCHANGE_PRODIGUER_OUT = "x-out"

# All exchanges.
EXCHANGES = set([
	EXCHANGE_PRODIGUER_EXT,
	EXCHANGE_PRODIGUER_IN,
	EXCHANGE_PRODIGUER_INTERNAL,
	EXCHANGE_PRODIGUER_OUT
	])

# Message queues.
QUEUE_EXT_LOG = "q-ext-log"
QUEUE_EXT_SMTP = "q-ext-smtp"
QUEUE_IN_LOG = "q-in-log"
QUEUE_IN_MONITORING_0000 = "q-in-monitoring-0000"
QUEUE_IN_MONITORING_0100 = "q-in-monitoring-0100"
QUEUE_IN_MONITORING_1000 = "q-in-monitoring-1000"
QUEUE_IN_MONITORING_1100 = "q-in-monitoring-1100"
QUEUE_IN_MONITORING_2000 = "q-in-monitoring-2000"
QUEUE_IN_MONITORING_3000 = "q-in-monitoring-3000"
QUEUE_IN_MONITORING_7000 = "q-in-monitoring-7000"
QUEUE_IN_MONITORING_8888 = "q-in-monitoring-8888"
QUEUE_IN_MONITORING_9000 = "q-in-monitoring-9000"
QUEUE_IN_MONITORING_9999 = "q-in-monitoring-9999"
QUEUE_INTERNAL_API = "q-internal-api"

# All queues.
QUEUES = set([
	QUEUE_EXT_LOG,
	QUEUE_EXT_SMTP,
	QUEUE_IN_LOG,
	QUEUE_IN_MONITORING_0000,
	QUEUE_IN_MONITORING_0100,
	QUEUE_IN_MONITORING_1000,
	QUEUE_IN_MONITORING_1100,
	QUEUE_IN_MONITORING_2000,
	QUEUE_IN_MONITORING_3000,
	QUEUE_IN_MONITORING_7000,
	QUEUE_IN_MONITORING_8888,
	QUEUE_IN_MONITORING_9000,
	QUEUE_IN_MONITORING_9999,
	QUEUE_INTERNAL_API
	])

# Message producers.
PRODUCER_IGCM = "libligcm"
PRODUCER_PRODIGUER = "prodiguer"

# All producers.
PRODUCERS = set([
	PRODUCER_IGCM,
	PRODUCER_PRODIGUER
	])

# Default message producer.
DEFAULT_PRODUCER = PRODUCER_IGCM

# Message server users.
USER_IGCM = "libligcm-mq-user"
USER_PRODIGUER = "prodiguer-mq-user"

# All users.
USERS = set([
	USER_IGCM,
	USER_PRODIGUER
	])

# Message application identifiers.
APP_MONITORING = "monitoring"

# All apps.
APPS = set([
	APP_MONITORING,
	])

# Default application.
DEFAULT_APP = APP_MONITORING

# Message types:
# ... general message types.
TYPE_GENERAL_SMTP = "-1000"
TYPE_GENERAL_API = "-2000"
# ... simulation monitoring.
TYPE_SMON_0000 = "0000"
TYPE_SMON_0100 = "0100"
TYPE_SMON_1000 = "1000"
TYPE_SMON_1100 = "1100"
TYPE_SMON_2000 = "2000"
TYPE_SMON_3000 = "3000"
TYPE_SMON_7000 = "7000"
TYPE_SMON_8888 = "8888"
TYPE_SMON_9000 = "9000"
TYPE_SMON_9999 = "9999"

# All types.
TYPES = set([
	TYPE_GENERAL_SMTP,
	TYPE_GENERAL_API,
	TYPE_SMON_0000,
	TYPE_SMON_0100,
	TYPE_SMON_1000,
	TYPE_SMON_1100,
	TYPE_SMON_2000,
	TYPE_SMON_3000,
	TYPE_SMON_7000,
	TYPE_SMON_8888,
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

# Default system mode.
DEFAULT_MODE = MODE_TEST

# Content types.
CONTENT_TYPE_JSON = 'application/json'
CONTENT_TYPE_BASE64 = 'application/base64'
CONTENT_TYPE_BASE64_JSON = 'application/base64+json'

# All content types.
CONTENT_TYPES = set([
	CONTENT_TYPE_JSON,
	CONTENT_TYPE_BASE64,
	CONTENT_TYPE_BASE64_JSON,
	])

# Default content type.
DEFAULT_CONTENT_TYPE = CONTENT_TYPE_JSON

# Content encodings.
CONTENT_ENCODING_UNICODE = "utf-8"

# All content encodings.
CONTENT_ENCODINGS = set([
	CONTENT_ENCODING_UNICODE
	])

# Default content encoding.
DEFAULT_CONTENT_ENCODING = CONTENT_ENCODING_UNICODE

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

# Default message priority.
DEFAULT_PRIORITY = PRIORITY_NORMAL

# Default publishing limit (used in automated tests).
DEFAULT_PUBLISH_LIMIT = 1

# Default publishing interval (used in automated tests).
DEFAULT_PUBLISH_INTERVAL = 0

# Default publishing frequency (used in automated tests).
DEFAULT_PUBLISH_FREQUENCY = 1

# Default message expiration.
DEFAULT_EXPIRATION = None

# Default delay (in seconds) before an MQ server connection
# retry is attempted.
DEFAULT_CONNECTION_REOPEN_DELAY = 5