# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.utils.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of message queue utilty functions.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>

"""
# Module imports.
import abc
import uuid

import pika

from . import constants
from ..utils import (
	config as cfg,
	convert,
	runtime as rt
	)



def connect(q):
	"""Binds to a mq subscriber channel.

	:param q: Name of message queue.
	:type q: str

	:returns: An mq channel for pub/sub operations.
	:rtype: pika.BlockingChannel

	"""
	rt.log_mq("CONNECTING", cfg.mq.host, q)

	# Get config.
	q_cfg = cfg.get_mq_config(q)

	connection = pika.BlockingConnection(pika.ConnectionParameters(
	               cfg.mq.host))
	channel = connection.channel()
	channel.queue_declare(queue=q_cfg.name, 
						  durable=q_cfg.is_durable)

	rt.log_mq("CONNECTED", cfg.mq.host, q)

	return channel, connection


def disconnect(q, connection):
	"""Binds to a mq subscriber channel.

	:param q: Name of message queue.
	:type q: str

	:returns: An mq channel for pub/sub operations.
	:rtype: pika.BlockingChannel

	"""
	rt.log_mq("DISCONNECTING", cfg.mq.host, q)

	connection.close()

	rt.log_mq("DISCONNECTED", cfg.mq.host, q)


def consume(q, cb, channel=None):
	"""Opens a mq subscriber channel and starts consuming messages.

	:param q: Name of message queue.
	:type q: str

	:param cb: Callback to invoke when a message is recieved from queue.
	:type cb: function

	:param channel: Message queue communications channel.
	:type channel: pika.BlockingChannel

	"""
	# Connect if necessary.
	if channel is None:
		channel, connection = connect(q)

	# Get config.
	q_cfg = cfg.get_mq_config(q)

	# Set up consumer.
	channel.basic_consume(cb, queue=q_cfg.name, no_ack=not q_cfg.acknowledge)

	# Start consuming.
	channel.start_consuming()


def publish(q, data, channel=None):
	"""Publishes data to an mq channel.


	:param q: Name of message queue.
	:type q: str

	:param data: Data to be published.
	:type data: dict

	:param channel: Message queue communications channel.
	:type channel: pika.BlockingChannel

	"""
	connection = None
	properties = None

	# Get config.
	q_cfg = cfg.get_mq_config(q)

	# Connect if necessary.
	if channel is None:
		channel, connection = connect(q)
	
	# Ensure message id durable.
	if q_cfg.is_durable:
		properties = pika.BasicProperties(
			content_type="application/json",
			content_encoding="utf-8",
			headers={
				"mode": "dev",
				"producer": "liblIGCM",
				"testing": "dna"
			},
			delivery_mode = 2,
			priority=1,
			correlation_id=unicode(uuid.uuid4()),
			reply_to=None,
			expiration="12345678",
			message_id=unicode(uuid.uuid4()),
			timestamp=convert.now_to_timestamp(),
			type="1000",
			user_id="guest",
			app_id="SMON",
			cluster_id=unicode(uuid.uuid4()),
			)
		properties.headers['test'] = 'test'
	
	# Publish over mq channel.
	channel.basic_publish(
		exchange='', 
		routing_key=q, 
		body=convert.dict_to_json(data),
		properties=properties)

	# Disconnect if using temporary connection.
	if connection is not None:
		disconnect(q, connection)


def create_ampq_message_properties(
	producer_id,
	app_id,
	message_type,
	message_id=unicode(uuid.uuid4()),
	headers={},
	cluster_id=None,
	content_encoding=constants.CONTENT_ENCODING_UNICODE,
	content_type=constants.CONTENT_TYPE_JSON,
	delivery_mode = constants.AMPQ_DELIVERY_MODE_PERSISTENT,
	expiration=constants.DEFAULT_EXPIRATION,
	mode=constants.DEFAULT_MODE,
	priority=constants.DEFAULT_PRIORITY,
	reply_to=None,
	timestamp=convert.now_to_timestamp(),
	user_id=constants.DEFAULT_USER):
	"""Factory function to return set of AMQP message properties.

	:param str producer_id: Message producer identifier.
	:param str app_id: Message application identifier.
	:param str message_type: Message type identifier.
	:param uuid message_id: Message unique identifier.
	:param dict headers: Custom message headers.
	:param str cluster_id: ID of MQ cluster.
	:param str content_encoding: Content encoding, e.g. utf-8.
	:param str content_type: Content MIME type, e.g. application/json.
	:param int delivery_mode: Message delivery mode (2 = with acknowledgement).
	:param int expiration: Ticks until message will no be considered as active.
	:param str mode: Messaging mode (dev|test|prod).
	:param int priority: Messaging priority.
	:param str reply_to: Messaging RPC callback.
	:param str timestamp: Timestamp.
	:param str user_id: ID of AMPQ user account under which messages are being dispatched.

	:returns pika.BasicProperties: Set of AMPQ message basic properties.

	"""
	# Defensive programming.
	if producer_id not in constants.PRODUCERS:
		raise ValueError("Unsupported producer identifier: {0}.".format(producer_id))
	if app_id not in constants.APPS:
		raise ValueError("Unsupported application identifier: {0}.".format(app_id))
	if message_type not in constants.TYPES:
		raise ValueError("Unsupported message type identifier: {0}.".format(message_type))
	if content_encoding not in constants.CONTENT_ENCODINGS:
		raise ValueError("Unsupported content encoding: {0}.".format(content_encoding))
	if content_type not in constants.CONTENT_TYPES:
		raise ValueError("Unsupported content type: {0}.".format(content_type))
	if delivery_mode not in constants.AMPQ_DELIVERY_MODES:
		raise ValueError("Unsupported delivery mode: {0}.".format(delivery_mode))
	if mode not in constants.MODES:
		raise ValueError("Unsupported mode: {0}.".format(mode))
	if priority not in constants.PRIORITIES:
		raise ValueError("Unsupported priority: {0}.".format(priority))

	# Default headers attached with each property.
	default_headers = {
		"mode": mode,
		"producer_id": producer_id	
	}

	# Return a pika BasicProperties instance (follows AMPQ protocol).
	return pika.BasicProperties(
		app_id=app_id,
		cluster_id=cluster_id,
		content_type=content_type,
		content_encoding=content_encoding,
		correlation_id=None,
		delivery_mode = delivery_mode,
		expiration=expiration,
		headers=dict(default_headers.items() + headers.items()),
		message_id=message_id,
		priority=priority,
		reply_to=reply_to,
		timestamp=timestamp,
		type=message_type,
		user_id=user_id
		)


class MessageInfo(object):
	"""Information used during message processing."""	
	def __init__(self, 
				 ampq_properties=None,
				 content=None):
		"""Constructor.

		:param pika.BasicProperties ampq_properties: Set of AMPQ properties associated with the message.
		:param object content: Message content.

		"""
		self.ampq_properties = ampq_properties
		self.content = content
		self.content_raw = content


	def parse_content(self):
		"""Parses message content.

		"""
		content_type = self.ampq_properties.content_type

		# Auto convert content to json.
		if content_type is None or \
		   content_type == constants.CONTENT_TYPE_JSON:
			self.content = convert.dict_to_json(self.content)


def parse(properties, content):
	"""Parses message properties / content.

	:param pika.BasicProperties properties: Set of message properties.
	:param unicode content: Message content.

	:returns unicode: Parsed message content / properties

	"""
	if properties.content_type is None or \
	   properties.content_type == constants.CONTENT_TYPE_JSON:
		content = convert.dict_to_json(content)

	return properties, content


class BaseProducer(object):
	"""This publisher handles unexpected interactions with RabbitMQ such as channel and connection closures.

	If RabbitMQ closes the connection, it will reopen it. You should
	look at the output, as there are limited reasons why the connection may
	be closed, which usually are tied to permission related issues or
	socket timeouts.

	It uses RabbitMQ delivery confirmations to verify that message was received by target broker.

	"""
	# Abstract Base Class module - see http://docs.python.org/library/abc.html
	__metaclass__ = abc.ABCMeta

	# Default attributes.
	MQ_EXCHANGE = constants.DEFAULT_EXCHANGE
	AMPQ_EXCHANGE_TYPE = constants.DEFAULT_EXCHANGE_TYPE
	PUBLISH_LIMIT = constants.DEFAULT_PUBLISH_LIMIT
	PUBLISH_INTERVAL = constants.DEFAULT_PUBLISH_INTERVAL


	def __init__(self, 
				 amqp_url = None, 
				 connection_reopen_delay = 5,
				 enable_confirmations = True):
		"""Setup the example publisher object, passing in the URL we will use
		to connect to RabbitMQ.

		:param str amqp_url: The URL for connecting to RabbitMQ
		:param bool enable_confirmations: Flag indicating whether message delivery confirmations are required.

		"""
		self._connection = None
		self._channel = None
		self._deliveries = []
		self._acked = 0
		self._nacked = 0
		self._message_number = 0
		self._stopping = False
		self._closing = False

		self._url = amqp_url or str(cfg.mq.connection)
		self._connection_reopen_delay = connection_reopen_delay
		self._enable_confirmations = enable_confirmations  
		self._stop_ioloop_on_disconnect = not (connection_reopen_delay > 0)


	def connect(self):
		"""This method connects to RabbitMQ, returning the connection handle.
		When the connection is established, the on_connection_open method
		will be invoked by pika. If you want the reconnection to work, make
		sure you set stop_ioloop_on_close to False, which is not the default
		behavior of this adapter.

		:rtype: pika.SelectConnection

		"""
		rt.log_mq('Connecting to {0}'.format(self._url))

		# Open up a non-blocking connection.
		return pika.SelectConnection(pika.URLParameters(self._url),
									 self.on_connection_open,
									 stop_ioloop_on_close=self._stop_ioloop_on_disconnect)


	def on_connection_open(self, unused_connection):
		"""This method is called by pika once the connection to RabbitMQ has
		been established. It passes the handle to the connection object in
		case we need it, but in this case, we'll just mark it unused.

		:param unused_connection: Unused pika AMPQ connection wrapper.
		:type unused_connection: pika.SelectConnection

		"""
		rt.log_mq('Connection opened')

		# Add connection close callback.
		self.add_on_connection_close_callback()

		# Open channel.
		self.open_channel()


	def add_on_connection_close_callback(self):
		"""This method adds an on close callback that will be invoked by pika
		when RabbitMQ closes the connection to the publisher unexpectedly.

		"""
		rt.log_mq('Adding connection close callback')

		# Event propogation.
		self._connection.add_on_close_callback(self.on_connection_closed)


	def on_connection_closed(self, connection, reply_code, reply_text):
		"""This method is invoked by pika when the connection to RabbitMQ is
		closed unexpectedly. Since it is unexpected, we will reconnect to
		RabbitMQ if it disconnects.

		:param pika.connection.Connection connection: The closed connection obj
		:param int reply_code: The server provided reply_code if given
		:param str reply_text: The server provided reply_text if given

		"""
		self._channel = None

		# Shutdown is in progress therefore kill io loop.
		if self._closing:
			self._connection.ioloop.stop()

		# Unexpected connection closure - attempt to reconnect.
		else:
			rt.log_mq('Connection closed, reopening in {0} seconds: ({1}) {2}'.format(self._connection_reopen_delay, reply_code, reply_text))
			self._connection.add_timeout(self._connection_reopen_delay, self.reconnect)


	def disconnect(self):
		"""This method closes the connection to RabbitMQ."""
		rt.log_mq('Closing connection')

		self._closing = True
		self._connection.close()


	def reconnect(self):
		"""Will be invoked by the IOLoop timer if the connection is
		closed. See the on_connection_closed method.

		"""
		# This is the old connection IOLoop instance, stop its ioloop
		self._connection.ioloop.stop()

		# Create a new connection
		self._connection = self.connect()

		# There is now a new connection, needs a new ioloop to run
		self._connection.ioloop.start()


	def open_channel(self):
		"""This method will open a new channel with RabbitMQ by issuing the
		Channel.Open RPC command. When RabbitMQ confirms the channel is open
		by sending the Channel.OpenOK RPC reply, the on_channel_open method
		will be invoked.

		"""
		rt.log_mq('Creating a new channel')

		# Open channel and handle callback.
		self._connection.channel(on_open_callback=self.on_channel_open)


	def on_channel_open(self, channel):
		"""This method is invoked by pika when the channel has been opened.
		The channel object is passed in so we can make use of it.

		Since the channel is now open, we'll declare the exchange to use.

		:param pika.channel.Channel channel: The channel object

		"""
		rt.log_mq('Channel opened')

		# Cache pointer.
		self._channel = channel

		# Add channel close callback.
		self.add_on_channel_close_callback()

		# Setup exchange.
		self.setup_exchange(self.MQ_EXCHANGE)


	def add_on_channel_close_callback(self):
		"""This method tells pika to call the on_channel_closed method if
		RabbitMQ unexpectedly closes the channel.

		"""
		rt.log_mq('Adding channel close callback')

		# Event propogation.
		self._channel.add_on_close_callback(self.on_channel_closed)


	def on_channel_closed(self, channel, reply_code, reply_text):
		"""Invoked by pika when RabbitMQ unexpectedly closes the channel.
		Channels are usually closed if you attempt to do something that
		violates the protocol, such as re-declare an exchange or queue with
		different parameters. In this case, we'll close the connection
		to shutdown the object.

		:param pika.channel.Channel: The closed channel
		:param int reply_code: The numeric reason the channel was closed
		:param str reply_text: The text reason the channel was closed

		"""
		rt.log_mq('Channel was closed: ({0}) {1}'.format(reply_code, reply_text))

		# Close connection (fires events).
		if not self._closing:
			self._connection.close()


	def close_channel(self):
		"""Invoke this command to close the channel with RabbitMQ by sending
		the Channel.Close RPC command.

		"""
		rt.log_mq('Closing the channel')

		# Close channel (fires events).
		if self._channel:
			self._channel.close()


	def setup_exchange(self, exchange_name):
		"""Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
		command. When it is complete, the on_exchange_declareok method will
		be invoked by pika.

		:param str|unicode exchange_name: The name of the exchange to declare

		"""
		rt.log_mq('Declaring exchange {0}'.format(exchange_name))

		self._channel.exchange_declare(self.on_exchange_declareok,
									   exchange_name,
									   self.AMPQ_EXCHANGE_TYPE)


	def on_exchange_declareok(self, unused_frame):
		"""Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
		command.

		:param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame

		"""
		rt.log_mq('Exchange declared')

		self.setup_queue(self.MQ_QUEUE)


	def setup_queue(self, q):
		"""Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
		command. When it is complete, the on_queue_declareok method will
		be invoked by pika.

		:param str|unicode q: The name of the queue to declare.

		"""
		rt.log_mq('Declaring queue {0}'.format(q))

		self._channel.queue_declare(self.on_queue_declareok, q)


	def on_queue_declareok(self, method_frame):
		"""Method invoked by pika when the Queue.Declare RPC call made in
		setup_queue has completed. In this method we will bind the queue
		and exchange together with the routing key by issuing the Queue.Bind
		RPC command. When this command is complete, the on_bindok method will
		be invoked by pika.

		:param pika.frame.Method method_frame: The Queue.DeclareOk frame

		"""
		rt.log_mq('Binding {0} to {2} with {2}'.format(self.MQ_EXCHANGE, self.MQ_QUEUE, self.MQ_ROUTING_KEY))

		self._channel.queue_bind(self.on_bindok, self.MQ_QUEUE, self.MQ_EXCHANGE, self.MQ_ROUTING_KEY)


	def on_bindok(self, unused_frame):
		"""This method is invoked by pika when it receives the Queue.BindOk
		response from RabbitMQ. Since we know we're now setup and bound, it's
		time to start publishing."""
		rt.log_mq('Queue bound')

		self.start_publishing()


	def start_publishing(self):
		"""This method will enable delivery confirmations and schedule the
		first message to be sent to RabbitMQ

		"""
		rt.log_mq('Issuing consumer related RPC commands')

		if self._enable_confirmations:
			self.enable_delivery_confirmations()
		self.schedule_next_message()        


	def enable_delivery_confirmations(self):
		"""Send the Confirm.Select RPC method to RabbitMQ to enable delivery
		confirmations on the channel. The only way to turn this off is to close
		the channel and create a new one.

		When the message is confirmed from RabbitMQ, the
		on_delivery_confirmation method will be invoked passing in a Basic.Ack
		or Basic.Nack method from RabbitMQ that will indicate which messages it
		is confirming or rejecting.

		"""
		rt.log_mq('Enabling delivery confirmations')

		self._channel.confirm_delivery(self.on_delivery_confirmation)


	def on_delivery_confirmation(self, method_frame):
		"""Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
		command, passing in either a Basic.Ack or Basic.Nack frame with
		the delivery tag of the message that was published. The delivery tag
		is an integer counter indicating the message number that was sent
		on the channel via Basic.Publish. Here we're just doing house keeping
		to keep track of stats and remove message numbers that we expect
		a delivery confirmation of from the list used to keep track of messages
		that are pending confirmation.

		:param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame

		"""
		confirmation_type = method_frame.method.NAME.split('.')[1].lower()

		rt.log_mq('Received {0} for delivery tag: {1}.'.format(confirmation_type, method_frame.method.delivery_tag))

		# Increment stats.
		# TODO - invoke unacknowledged callback
		if confirmation_type == 'ack':
			self._acked += 1
		elif confirmation_type == 'nack':
			self._nacked += 1

		# Remove delivery tag.
		self._deliveries.remove(method_frame.method.delivery_tag)

		msg = "Publishing stats: all = {0}; unconfirmed = {1}; acknowledged = {2}; unacknowledged = {3}."
		msg = msg.format(self._message_number, len(self._deliveries), self._acked, self._nacked)
		rt.log_mq(msg)


	def schedule_next_message(self):
		"""If we are not closing our connection to RabbitMQ, schedule another
		message to be delivered in PUBLISH_INTERVAL seconds.

		"""
		if self._stopping:
			return

		# Stop when publishing limit is reached.
		if self.PUBLISH_LIMIT and self.PUBLISH_LIMIT == self._message_number:
			rt.log_mq('Stopping N-time publisher.')
			self.stop()

		# Next (timed).
		elif self.PUBLISH_INTERVAL:
			rt.log_mq('Scheduling next message for {0} seconds.'.format(self.PUBLISH_INTERVAL))
			self._connection.add_timeout(self.PUBLISH_INTERVAL, self.publish_message)

		# Next.
		else:
			rt.log_mq('Scheduling publishing.')
			self._connection.add_timeout(0, self.publish_message)


	def _publish(self, type_id, content):
		# Create properties.
		props = create_properties(type_id, self)

		# Parse.
		props, content = parse(props, content)

		# Publish over MQ channel.
		self._channel.basic_publish(self.MQ_EXCHANGE, 
									self.MQ_ROUTING_KEY,
									content,
									props)

		# Increment stats.
		self._message_number += 1
		self._deliveries.append(self._message_number)

		rt.log_mq('Published message # {0}.'.format(self._message_number))


	def publish_message(self):
		"""If the class is not stopping, publish a message to RabbitMQ,
		appending a list of deliveries with the message number that was sent.
		This list will be used to check for delivery confirmations in the
		on_deliveryenable_delivery_confirmations method.

		Once the message has been sent, schedule another message to be sent.
		The main reason I put scheduling in was just so you can get a good idea
		of how the process is flowing by slowing down and speeding up the
		delivery intervals by changing the PUBLISH_INTERVAL constant in the
		class.

		"""
		if self._stopping:
			return

		# Iterate messages emitted by producer.	
		try:
			for type_id, content in self.get_message():
				self._publish(type_id, content)
		except StopIteration:
			pass

		# Schedule next message.
		self.schedule_next_message()


	def run(self):
		"""Launches message production by connecting and then starting the IOLoop.

		"""
		self._connection = self.connect()
		self._connection.ioloop.start()


	def stop(self):
		"""Stops message production by closing the channel and connection..

		"""
		rt.log_mq('Stopping')

		# Set flag so as to prevent further publishing.
		self._stopping = True

		# Close resources.
		self.close_channel()
		self.disconnect()

		# Restart io loop to allow handle KeyboardInterrupt scenario.
		self._connection.ioloop.start()

		rt.log_mq('Stopped')


	def get_message():
		"""Returns next message for processing.

		"""
		pass


	@classmethod
	def publish(cls):
		"""Invokes publisher.

		"""
		# Instantiate and verify type.
		producer = cls()
		if not isinstance(producer, BaseProducer):
			raise TypeError("Producer is not a BaseProducer sub-class.")

		# Run.
		try:
			producer.run()
		except KeyboardInterrupt:
			producer.stop()

