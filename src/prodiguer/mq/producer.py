# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.producer.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Message queue producer - publishes messages to an MQ server.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
import pika

from prodiguer.mq import defaults
from prodiguer.utils import config
from prodiguer.utils import logger



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
                 connection_url=None,
                 enable_confirmations=True,
                 publish_limit=defaults.DEFAULT_PUBLISH_LIMIT,
                 publish_interval=defaults.DEFAULT_PUBLISH_INTERVAL,
                 verbose=False):
        """Setup the example publisher object, passing in the URL we will use
        to connect to RabbitMQ.

        :param msg_source: Source of messages for publishing.
        :type msg_source: Message | function
        :param str connection_url: An MQ server connection URL.
        :param bool enable_confirmations: Flag indicating whether message delivery confirmations are required.
        :param int publish_limit: Maximum number of message publishing events.
        :param int publish_interval: Frequency at which message(s) are published.
        :param bool verbose: Flag indicating whether logging level is verbose or not.

        """
        # Override defaults from config.
        if connection_url is None:
            connection_url=config.mq.connections.main

        self._msg_source = msg_source
        self._connection_reopen_delay = defaults.DEFAULT_CONNECTION_REOPEN_DELAY
        self._enable_confirmations = enable_confirmations
        self._publish_limit = publish_limit
        self._publish_interval = publish_interval
        self._stop_ioloop_on_disconnect = not (self._connection_reopen_delay > 0)
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


    def _log(self, msg, force=False):
        """Logging helper function.

        """
        if self._verbose or force:
            logger.log_mq(msg)


    def _connect(self):
        """This method connects to MQ server, returning the connection handle.
        When the connection is established, the on_connection_open method
        will be invoked by pika. If you want the reconnection to work, make
        sure you set stop_ioloop_on_close to False, which is not the default
        behavior of this adapter.

        :rtype: pika.SelectConnection

        """
        self._log('Connecting to {0}'.format(self._url))

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
        self._log('Connection opened')

        # Add connection close callback.
        self._add_on_connection_close_callback()

        # Open channel.
        self._open_channel()


    def _add_on_connection_close_callback(self):
        """This method adds an on close callback that will be invoked by pika
        when the MQ server closes the connection to the publisher unexpectedly.

        """
        self._log('Adding connection close callback')

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
            msg = "Connection closed, reopening in {0} seconds: ({1}) {2}"
            msg = msg.format(self._connection_reopen_delay,
                             reply_code,
                             reply_text)
            self._log(msg)
            self._connection.add_timeout(self._connection_reopen_delay,
                                         self._reconnect)


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


    def _disconnect(self):
        """This method closes the connection to RabbitMQ."""
        self._log('Closing connection')

        self._closing = True
        self._connection.close()


    def _open_channel(self):
        """This method will open a new channel with RabbitMQ by issuing the
        Channel.Open RPC command. When RabbitMQ confirms the channel is open
        by sending the Channel.OpenOK RPC reply, the on_channel_open method
        will be invoked.

        """
        self._log('Creating a new channel')

        # Open channel and handle callback.
        self._connection.channel(on_open_callback=self._on_channel_open)


    def _on_channel_open(self, channel):
        """This method is invoked by pika when the channel has been opened.
        The channel object is passed in so we can make use of it.

        Since the channel is now open, we'll declare the exchange to use.

        :param pika.channel.Channel channel: The channel object

        """
        self._log('Channel opened')

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
        self._log('Adding channel close callback')

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
        msg = "Channel was closed: ({0}) {1}"
        msg = msg.format(reply_code, reply_text)
        self._log(msg)

        # Close connection (fires events).
        if not self._closing:
            self._connection.close()


    def _close_channel(self):
        """Invoke this command to close the channel with RabbitMQ by sending
        the Channel.Close RPC command.

        """
        self._log('Closing the channel')

        # Close channel (fires events).
        if self._channel:
            self._channel.close()


    def _start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ

        """
        self._log('Issuing consumer related RPC commands')

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
        self._log('Enabling delivery confirmations')

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

        self._log('Received {0} for message: {1}.'.format(confirmation_type, method_frame.method.delivery_tag))

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
            self._log('Stopping N-time publisher.')
            self.stop()

        # Next (timed).
        elif self._publish_interval > 0:
            self._log('Scheduling next message for {0} seconds.'.format(self._publish_interval))
            self._connection.add_timeout(self._publish_interval, self._publish)

        # Next.
        else:
            self._log('Scheduling publishing.')
            self._connection.add_timeout(0, self._publish)


    def _publish_message(self, msg):
        """Publishes an individual message."""
        # Encode content in readiness for publishing.
        msg.encode()

        # Publish over MQ channel.
        self._channel.basic_publish(msg.exchange,
                                    msg.routing_key,
                                    msg.content,
                                    msg.props)

        # Increment stats.
        self._message_count += 1
        self._deliveries.append(self._message_count)

        self._log('Published message # {0}.'.format(self._message_count))


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
        try:
            for msg in self._msg_source():
                yield msg
        except TypeError:
            try:
                for msg in self._msg_source:
                    yield msg
            except TypeError:
                yield self._msg_source


    def run(self, on_publish_callback=None):
        """Launches message production by connecting and then starting the IOLoop.

        """
        self._connection = self._connect()
        self._connection.ioloop.start()


    def stop(self):
        """Stops message production by closing the channel and connection..

        """
        self._log('Stopping')

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
        self._log(msg)

        # Restart io loop to allow handle KeyboardInterrupt scenario.
        self._connection.ioloop.start()

        self._log('Stopped')
