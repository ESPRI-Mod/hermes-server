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
import inspect, uuid

import pika

from . import constants
from ..utils import (
    config as cfg,
    convert,
    runtime as rt
    )



class Message(object):
    """Wraps a message either being consumed or produced."""
    def __init__(self, exchange, props, content):
        """Constructor.

        :param str exchange: Target message exchange.
        :param pika.BasicProperties props: Set of AMPQ properties associated with the message.
        :param object content: Message content.

        """
        # Validate inputs.
        if not isinstance(props, pika.BasicProperties):
            raise ValueError("AMPQ message basic properties is invalid.")
        if 'mode' not in props.headers:
            msg = "[mode] is a required header field."
            raise ValueError(msg)
        if props.headers['mode'] not in constants.MODES:
            msg = "Unsupported mode: {0}."
            raise ValueError(msg.format(props.headers['mode']))
        if 'producer_id' not in props.headers:
            msg = "[producer_id] is a required header field."
            raise ValueError(msg)
        if props.headers['producer_id'] not in constants.PRODUCERS:
            msg = "Unsupported producer_id: {0}."
            raise ValueError(msg.format(props.headers['producer_id']))


        self.exchange = exchange
        self.content = content
        self.content_raw = content
        self.content_type = props.content_type
        self.props = props
        self.routing_key = "{0}.{1}.{2}.{3}.{4}".format(props.headers['mode'],
                                                        props.user_id,
                                                        props.headers['producer_id'],
                                                        props.app_id,
                                                        props.type).lower()


    def parse_content(self):
        """Parses message content.

        """
        # Auto convert content to json.
        if self.content_type in (None, constants.CONTENT_TYPE_JSON):
            self.content = convert.dict_to_json(self.content)


def create_ampq_message_properties(
    user_id,
    producer_id,
    app_id,
    message_type,
    message_id=unicode(uuid.uuid4()),
    headers={},
    cluster_id=None,
    content_encoding=constants.DEFAULT_CONTENT_ENCODING,
    content_type=constants.DEFAULT_CONTENT_TYPE,
    delivery_mode = constants.DEFAULT_DELIVERY_MODE,
    expiration=constants.DEFAULT_EXPIRATION,
    mode=constants.DEFAULT_MODE,
    priority=constants.DEFAULT_PRIORITY,
    reply_to=None,
    timestamp=convert.now_to_timestamp()):
    """Factory function to return set of AMQP message properties.

    :param str user_id: ID of AMPQ user account under which messages are being dispatched.
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

    :returns pika.BasicProperties: Set of AMPQ message basic properties.

    """
    # Validate inputs.
    if producer_id not in constants.PRODUCERS:
        msg = "Unsupported producer identifier: {0}."
        raise ValueError(msg.format(producer_id))
    if app_id not in constants.APPS:
        msg = "Unsupported application identifier: {0}."
        raise ValueError(msg.format(app_id))
    if message_type not in constants.TYPES:
        msg = "Unsupported message type identifier: {0}."
        raise ValueError(msg.format(message_type))
    if user_id not in constants.USERS:
        msg = "Unsupported user identifier: {0}."
        raise ValueError(msg.format(user_id))
    if content_encoding not in constants.CONTENT_ENCODINGS:
        msg = "Unsupported content encoding: {0}."
        raise ValueError(msg.format(content_encoding))
    if content_type not in constants.CONTENT_TYPES:
        msg = "Unsupported content type: {0}."
        raise ValueError(msg.format(content_type))
    if delivery_mode not in constants.AMPQ_DELIVERY_MODES:
        msg = "Unsupported delivery mode: {0}."
        raise ValueError(msg.format(delivery_mode))
    if mode not in constants.MODES:
        msg = "Unsupported mode: {0}."
        raise ValueError(msg.format(mode))
    if priority not in constants.PRIORITIES:
        msg = "Unsupported priority: {0}."
        raise ValueError(msg.format(priority))


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


class Producer(object):
    """This publisher handles unexpected interactions with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    It uses RabbitMQ delivery confirmations to verify that message was received by target broker.

    """
    def __init__(self,
                 msg_source,
                 connection_url=cfg.mq.connection,
                 connection_reopen_delay=cfg.mq.connection_reopen_delay,
                 enable_confirmations=True,
                 publish_limit=constants.DEFAULT_PUBLISH_LIMIT,
                 publish_interval=constants.DEFAULT_PUBLISH_INTERVAL,
                 verbose=False):
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.

        :param msg_source: Source of messages for publishing.
        :type msg_source: Message | function
        :param str connection_url: An MQ server connection URL.
        :param int connection_reopen_delay: Delay in seconds before a connection is reopened after somekind of issue.
        :param bool enable_confirmations: Flag indicating whether message delivery confirmations are required.
        :param int publish_limit: Maximum number of message publishing events.
        :param int publish_interval: Frequency at which message(s) are published.
        :param bool verbose: Flag indicating whether logging level is verbose or not.

        """
        self._msg_source = msg_source
        self._connection_reopen_delay = connection_reopen_delay
        self._enable_confirmations = enable_confirmations
        self._publish_limit = publish_limit
        self._publish_interval = publish_interval
        self._stop_ioloop_on_disconnect = not (connection_reopen_delay > 0)
        self._url = connection_url
        self._verbose = verbose

        self._acked = 0
        self._closing = False
        self._channel = None
        self._connection = None
        self._deliveries = []
        self._message_count = 0
        self._nacked = 0
        self._published_count = 0
        self._stopping = False


    def _connect(self):
        """This method connects to MQ server, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika. If you want the reconnection to work, make
        sure you set stop_ioloop_on_close to False, which is not the default
        behavior of this adapter.

        :rtype: pika.SelectConnection

        """
        if self._verbose:
            rt.log_mq('Connecting to {0}'.format(self._url))

        # Open up a non-blocking connection.
        return pika.SelectConnection(
            pika.URLParameters(str(self._url)),
            self._on_connection_open,
            stop_ioloop_on_close=self._stop_ioloop_on_disconnect
            )


    def _on_connection_open(self, unused_connection):
        """This method is called by pika once the connection to the MQ server has been established.
        It passes the handle to the connection object in case we need it,
        but in this case, we'll just mark it unused.

        :param unused_connection: Unused pika AMPQ connection wrapper.
        :type unused_connection: pika.SelectConnection

        """
        if self._verbose:
            rt.log_mq('Connection opened')

        # Add connection close callback.
        self._add_on_connection_close_callback()

        # Open channel.
        self._open_channel()


    def _add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when the MQ server closes the connection to the publisher unexpectedly.

        """
        if self._verbose:
            rt.log_mq('Adding connection close callback')

        # Event propogation.
        self._connection.add_on_close_callback(self._on_connection_closed)


    def _on_connection_closed(self, connection, reply_code, reply_text):
        """This method is invoked by pika when the MQ server connection is
        closed unexpectedly. Since it is unexpected, we will reconnect to
        MQ server if it disconnects.

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
            if self._verbose:
                msg = "Connection closed, reopening in {0} seconds: ({1}) {2}"
                msg = msg.format(self._connection_reopen_delay, reply_code, reply_text)
                rt.log_mq(msg)
            self._connection.add_timeout(self._connection_reopen_delay, self._reconnect)


    def _disconnect(self):
        """This method closes the connection to RabbitMQ."""
        if self._verbose:
            rt.log_mq('Closing connection')

        self._closing = True
        self._connection.close()


    def _reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the on_connection_closed method.

        """
        # This is the old connection IOLoop instance, stop its ioloop
        self._connection.ioloop.stop()

        # Create a new connection
        self._connection = self._connect()

        # There is now a new connection, needs a new ioloop to run
        self._connection.ioloop.start()


    def _open_channel(self):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.

        """
        if self._verbose:
            rt.log_mq('Creating a new channel')

        # Open channel and handle callback.
        self._connection.channel(on_open_callback=self._on_channel_open)


    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        if self._verbose:
            rt.log_mq('Channel opened')

        # Cache pointer.
        self._channel = channel

        # Add channel close callback.
        self._add_on_channel_close_callback()

        # Start publishing.
        self._start_publishing()


    def _add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.

        """
        if self._verbose:
            rt.log_mq('Adding channel close callback')

        # Event propogation.
        self._channel.add_on_close_callback(self._on_channel_closed)


    def _on_channel_closed(self, channel, reply_code, reply_text):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.

        :param pika.channel.Channel: The closed channel
        :param int reply_code: The numeric reason the channel was closed
        :param str reply_text: The text reason the channel was closed

        """
        if self._verbose:
            rt.log_mq('Channel was closed: ({0}) {1}'.format(reply_code, reply_text))

        # Close connection (fires events).
        if not self._closing:
            self._connection.close()


    def _close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.

        """
        if self._verbose:
            rt.log_mq('Closing the channel')

        # Close channel (fires events).
        if self._channel:
            self._channel.close()


    def _start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        if self._verbose:
            rt.log_mq('Issuing consumer related RPC commands')

        if self._enable_confirmations:
            self._enable_delivery_confirmations()
        self._schedule_next_message()


    def _enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.

        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.

        """
        if self._verbose:
            rt.log_mq('Enabling delivery confirmations')

        self._channel.confirm_delivery(self._on_delivery_confirmation)


    def _on_delivery_confirmation(self, method_frame):
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

        if self._verbose:
            rt.log_mq('Received {0} for message: {1}.'.format(confirmation_type, method_frame.method.delivery_tag))

        # Increment stats.
        # TODO - invoke unacknowledged callback
        if confirmation_type == 'ack':
            self._acked += 1
        elif confirmation_type == 'nack':
            self._nacked += 1

        # Remove delivery tag.
        self._deliveries.remove(method_frame.method.delivery_tag)


    def _schedule_next_message(self):
        """If we are not closing our connection to RabbitMQ, schedule another
        message to be delivered in PUBLISH_INTERVAL seconds.

        """
        if self._stopping:
            return

        # Stop when publishing limit is reached.
        if self._publish_limit > 0 and \
           self._publish_limit == self._published_count:
            if self._verbose:
                rt.log_mq('Stopping N-time publisher.')
            self.stop()

        # Next (timed).
        elif self._publish_interval > 0:
            if self._verbose:
                rt.log_mq('Scheduling next message for {0} seconds.'.format(self._publish_interval))
            self._connection.add_timeout(self._publish_interval, self._publish)

        # Next.
        else:
            if self._verbose:
                rt.log_mq('Scheduling publishing.')
            self._connection.add_timeout(0, self._publish)


    def _publish_message(self, msg):
        """Publishes an individual message."""
        # Parse.
        msg.parse_content()

        # Publish over MQ channel.
        self._channel.basic_publish(msg.exchange,
                                    msg.routing_key,
                                    msg.content,
                                    msg.props)

        # Increment stats.
        self._message_count += 1
        self._deliveries.append(self._message_count)

        if self._verbose:
            rt.log_mq('Published message # {0}.'.format(self._message_count))


    def _publish(self):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        enable_delivery_confirmations method.

        Once the message has been sent, schedule another message to be sent.
        The main reason I put scheduling in was just so you can get a good idea
        of how the process is flowing by slowing down and speeding up the
        delivery intervals by changing the PUBLISH_INTERVAL constant in the
        class.

        """
        if self._stopping:
            return

        # Increment publish count.
        self._published_count += 1

        # Iterate messages emitted by producer.
        try:
            for msg in self._get_messages():
                self._publish_message(msg)
        except StopIteration:
            pass

        # Schedule next message.
        self._schedule_next_message()


    def _get_messages(self):
        """Returns next message(s) for processing.

        """
        if inspect.isfunction(self._msg_source):
            for msg in self._msg_source():
                yield msg
        else:
            yield self._msg_source



    def run(self, on_publish_callback=None):
        """Launches message production by connecting and then starting the IOLoop.

        """
        self._connection = self._connect()
        self._connection.ioloop.start()


    def stop(self):
        """Stops message production by closing the channel and connection..

        """
        if self._verbose:
            rt.log_mq('Stopping')

        # Set flag so as to prevent further publishing.
        self._stopping = True

        # Close resources.
        self._close_channel()
        self._disconnect()

        # Log stats.
        msg = "Publishing stats: all = {0}; unconfirmed = {1}; "
        msg += "acknowledged = {2}; unacknowledged = {3}."
        msg = msg.format(self._message_count, len(self._deliveries),
                         self._acked, self._nacked)
        rt.log_mq(msg)

        # Restart io loop to allow handle KeyboardInterrupt scenario.
        self._connection.ioloop.start()

        if self._verbose:
            rt.log_mq('Stopped')


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


def publish(msg_source,
            connection_url=cfg.mq.connection,
            connection_reopen_delay=cfg.mq.connection_reopen_delay,
            enable_confirmations=True,
            publish_limit=constants.DEFAULT_PUBLISH_LIMIT,
            publish_interval=constants.DEFAULT_PUBLISH_INTERVAL,
            verbose=False):
    """Publishes message(s) to MQ server.

    :param msg_source: Source of messages for publishing.
    :type msg_source: Message | function
    :param str connection_url: An MQ server connection URL.
    :param int connection_reopen_delay: Delay in seconds before a connection is reopened after somekind of issue.
    :param bool enable_confirmations: Flag indicating whether message delivery confirmations are required.
    :param int publish_limit: Maximum number of message publishing events.
    :param int publish_interval: Frequency at which message(s) are published.
    :param bool verbose: Flag indicating whether logging level is verbose or not.

    """
    # Instantiate producer.
    producer = Producer(msg_source,
                        connection_url,
                        connection_reopen_delay,
                        enable_confirmations,
                        publish_limit,
                        publish_interval,
                        verbose)

    # Run.
    try:
        producer.run()
    except KeyboardInterrupt:
        producer.stop()
