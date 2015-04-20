# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.constants.py
   :platform: Unix
   :synopsis: Prodiguer mq constants.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


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
QUEUE_EXT_SMTP = "q-ext-smtp"
QUEUE_IN_MONITORING = "q-in-monitoring"
QUEUE_IN_MONITORING_0000 = "q-in-monitoring-0000"
QUEUE_IN_MONITORING_0100 = "q-in-monitoring-0100"
QUEUE_IN_MONITORING_1000 = "q-in-monitoring-1000"
QUEUE_IN_MONITORING_1100 = "q-in-monitoring-1100"
QUEUE_IN_MONITORING_2000 = "q-in-monitoring-2000"
QUEUE_IN_MONITORING_2100 = "q-in-monitoring-2000"
QUEUE_IN_MONITORING_3000 = "q-in-monitoring-3000"
QUEUE_IN_MONITORING_3100 = "q-in-monitoring-3100"
QUEUE_IN_MONITORING_4000 = "q-in-monitoring-4000"
QUEUE_IN_MONITORING_4100 = "q-in-monitoring-4100"
QUEUE_IN_MONITORING_4900 = "q-in-monitoring-4900"
QUEUE_IN_MONITORING_7000 = "q-in-monitoring-7000"
QUEUE_IN_MONITORING_7100 = "q-in-monitoring-7100"
QUEUE_IN_MONITORING_8888 = "q-in-monitoring-8888"
QUEUE_IN_MONITORING_9999 = "q-in-monitoring-9999"
QUEUE_INTERNAL_API = "q-internal-api"
QUEUE_INTERNAL_CV = "q-internal-cv"
QUEUE_INTERNAL_SMTP = "q-internal-smtp"
QUEUE_INTERNAL_SMS = "q-internal-sms"

# All queues.
QUEUES = set([
	QUEUE_EXT_SMTP,
	QUEUE_IN_MONITORING,
	QUEUE_IN_MONITORING_0000,
	QUEUE_IN_MONITORING_0100,
	QUEUE_IN_MONITORING_1000,
	QUEUE_IN_MONITORING_1100,
	QUEUE_IN_MONITORING_2000,
	QUEUE_IN_MONITORING_2100,
	QUEUE_IN_MONITORING_3000,
	QUEUE_IN_MONITORING_3100,
	QUEUE_IN_MONITORING_4000,
	QUEUE_IN_MONITORING_4100,
	QUEUE_IN_MONITORING_4900,
	QUEUE_IN_MONITORING_7000,
	QUEUE_IN_MONITORING_7100,
	QUEUE_IN_MONITORING_8888,
	QUEUE_IN_MONITORING_9999,
	QUEUE_INTERNAL_API,
	QUEUE_INTERNAL_CV,
	QUEUE_INTERNAL_SMTP,
	QUEUE_INTERNAL_SMS
	])

# Message producers.
PRODUCER_IGCM = "libigcm"
PRODUCER_PRODIGUER = "prodiguer"

# All producers.
PRODUCERS = set([
	PRODUCER_IGCM,
	PRODUCER_PRODIGUER
	])

# Message server users.
USER_IGCM = "libigcm-mq-user"
USER_PRODIGUER = "prodiguer-mq-user"

# All users.
USERS = set([
	USER_IGCM,
	USER_PRODIGUER
	])

# Message application identifiers.
APP_MONITORING = "monitoring"
APP_SYSTEM_METRICS = "sys-metrics"
APP_SIMULATION_METRICS = "sim-metrics"

# All apps.
APPS = set([
	APP_MONITORING,
	APP_SYSTEM_METRICS,
	APP_SIMULATION_METRICS
	])

# Message types:
# ... general message types.
TYPE_GENERAL_API = "-2000"
TYPE_GENERAL_CV = "-4000"
TYPE_GENERAL_SMTP = "-1000"
TYPE_GENERAL_SMS = "-3000"
# ... simulation monitoring.
TYPE_SMON_0000 = "0000"		# Simulation initialiation
TYPE_SMON_0100 = "0100"		# Simulation ends
TYPE_SMON_1000 = "1000"		# Compute job begins
TYPE_SMON_1100 = "1100"		# Compute job ends
TYPE_SMON_2000 = "2000"		# Post-processing job begins
TYPE_SMON_2100 = "2100"		# Post-processing job ends
TYPE_SMON_3000 = "3000"		# Post-processing job from checker begins
TYPE_SMON_3100 = "3100"		# Post-processing job from checker ends
TYPE_SMON_4000 = "4000"		# Push stack
TYPE_SMON_4100 = "4100"		# Pop stack
TYPE_SMON_4900 = "4900"		# Pop stack failure
TYPE_SMON_9000 = "9000"		# Pop stack failure - TODO deprecate
TYPE_SMON_7000 = "7000"
TYPE_SMON_7100 = "7100"
TYPE_SMON_8888 = "8888"		# Cleanup
TYPE_SMON_9999 = "9999"		# Simulation stopped due to error

# All types.
TYPES = set([
	TYPE_GENERAL_API,
	TYPE_GENERAL_CV,
	TYPE_GENERAL_SMS,
	TYPE_GENERAL_SMTP,
	TYPE_SMON_0000,
	TYPE_SMON_0100,
	TYPE_SMON_1000,
	TYPE_SMON_1100,
	TYPE_SMON_2000,
	TYPE_SMON_2100,
	TYPE_SMON_3000,
	TYPE_SMON_3100,
	TYPE_SMON_4000,
	TYPE_SMON_4100,
	TYPE_SMON_4900,
	TYPE_SMON_9000,
	TYPE_SMON_7000,
	TYPE_SMON_7100,
	TYPE_SMON_8888,
	TYPE_SMON_9999,
	])

# Timestamp precision types.
TIMESTAMP_PRECISION_NS = 'ns'
TIMESTAMP_PRECISION_MS = 'ms'

# All timestamp precision types.
TIMESTAMP_PRECISIONS = set([
	TIMESTAMP_PRECISION_NS,
	TIMESTAMP_PRECISION_MS
	])

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

