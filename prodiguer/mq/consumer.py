# -*- coding: utf-8 -*-

"""
.. module:: hermes.mq.consumer.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Message queue consumer - reads messages from an MQ server.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
import inspect

import pika

from hermes.mq import constants
from hermes.mq import defaults
from hermes.mq import message
from hermes.utils import config
from hermes.utils import logger



class Consumer(object):
    """This is an example consumer that will handle unexpected interactions
    with RabbitMQ such as channel and connection closures.

    If RabbitMQ closes the connection, it will reopen it. You should
    look at the output, as there are limited reasons why the connection may
    be closed, which usually are tied to permission related issues or
    socket timeouts.

    If the channel is closed, it will indicate a problem with one of the
    commands that were issued and that should surface in the output as well.

    """
    def __init__(self,
                 exchange,
                 queue,
                 callback,
                 connection_url=None,
                 consume_limit=0,
                 context_type=message.Message,
                 verbose=False):
        """Create a new instance of the consumer class, passing in the AMQP
        URL used to connect to RabbitMQ.

        :param str exchange: Name of an exchange to bind to.
        :param str queue: Name of queue to bind to.
        :param func callback: Function to invoke when message has been handled.

        :param str connection_url: An MQ server connection URL.
        :param int consume_limit: Limit upon number of message to be consumed.
        :param class context_type: Type of message processing context object to instantiate.
        :param bool verbose: Flag indicating whether logging level is verbose or not.

        """
        # Validate inputs.
        if exchange not in constants.EXCHANGES:
            err = "Invalid MQ exchange: {0}".format(exchange)
            raise ValueError(err)
        if queue not in constants.QUEUES:
            err = "Invalid MQ queue: {0}".format(queue)
            raise ValueError(err)
        if not inspect.isfunction(callback):
            err = "Invalid message callback handler"
            raise ValueError(err)
        if not issubclass(context_type, message.Message):
            err = "Invalid message processing context type"
            raise ValueError(err)

        # Override inputs from config.
        if connection_url is None:
            connection_url = config.mq.connections.main

        # Initialize properties from inputs.
        self._callback = callback
        self._connection_reopen_delay = defaults.DEFAULT_CONNECTION_REOPEN_DELAY
        self._consume_limit = consume_limit
        self._context_type = context_type
        self._exchange = exchange
        self._queue = queue
        self._stop_ioloop_on_disconnect = not self._connection_reopen_delay > 0
        self._url = connection_url
        self._verbose = verbose

        # Initialize other properties.
        self._channel = None
        self._closing = False
        self._connection = None
        self._consumed = 0
        self._consumer_tag = None


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
            pika.URLParameters(self._url),
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

        # Bind to queue.
        self._bind_to_queue()


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


    def _bind_to_queue(self):
        """Binds to target queue."""
        msg = "Binding {0} to {1}"
        msg = msg.format(self._exchange, self._queue)
        self._log(msg)

        self._channel.queue_bind(self._on_queue_bindok,
                                 self._queue,
                                 self._exchange)


    def _on_queue_bindok(self, unused_frame):
        """Invoked by pika when the Queue.Bind method has completed. At this
        point we will start consuming messages by calling start_consuming
        which will invoke the needed RPC commands to start the process.

        :param pika.frame.Method unused_frame: The Queue.BindOk response frame

        """
        self._log("Queue bound")
        self._start_consuming()


    def _start_consuming(self):
        """This method sets up the consumer by first calling
        add_on_cancel_callback so that the object is notified if RabbitMQ
        cancels the consumer. It then issues the Basic.Consume RPC command
        which returns the consumer tag that is used to uniquely identify the
        consumer with RabbitMQ. We keep the value to use it when we want to
        cancel consuming. The on_message method is passed in as a callback pika
        will invoke when a message is fully received.

        """
        self._log("Issuing consumer related RPC commands")
        self._add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(self._on_message,
                                                         self._queue)


    def _add_on_cancel_callback(self):
        """Add a callback that will be invoked if RabbitMQ cancels the consumer
        for some reason. If RabbitMQ does cancel the consumer,
        on_consumer_cancelled will be invoked by pika.

        """
        self._log("Adding consumer cancellation callback")
        self._channel.add_on_cancel_callback(self._on_consumer_cancelled)


    def _on_consumer_cancelled(self, method_frame):
        """Invoked by pika when RabbitMQ sends a Basic.Cancel for a consumer
        receiving messages.

        :param pika.frame.Method method_frame: The Basic.Cancel frame

        """
        msg = "Consumer was cancelled remotely, shutting down: {0}"
        msg = msg.format(method_frame)
        self._log(msg)

        if self._channel:
            self._channel.close()


    def _on_message(self,
                    unused_channel,
                    basic_deliver,
                    properties,
                    body):
        """Invoked by pika when a message is delivered from RabbitMQ. The
        channel is passed for your convenience. The basic_deliver object that
        is passed in carries the exchange, routing key, delivery tag and
        a redelivered flag for the message. The properties passed in is an
        instance of BasicProperties with the message properties and the body
        is the message that was sent.

        :param pika.channel.Channel unused_channel: The channel object
        :param pika.Spec.Basic.Deliver: basic_deliver method
        :param pika.Spec.BasicProperties: properties
        :param str|unicode body: The message body

        """
        # Escape if already at consumption limit.
        if self._consume_limit > 0 and \
           self._consumed == self._consume_limit:
           return

        # Increment consumed count.
        self._consumed += 1

        # Log.
        msg = "Received message # {0}::{1} from {2}"
        msg = msg.format(basic_deliver.delivery_tag,
                         properties.message_id,
                         properties.app_id)
        self._log(msg)

        # Invoke callback.
        self._process_message(properties, body)

        # Acknowledge message.
        self._acknowledge_message(basic_deliver.delivery_tag)

        # Disconnect if consumption limit reached.
        if self._consume_limit > 0 and \
           self._consumed == self._consume_limit:
           self._disconnect()


    def _process_message(self, props, payload):
        """Processes message being consumed.

        """
        ctx = self._context_type(props, payload)
        ctx.decode()
        self._callback(ctx)


    def _acknowledge_message(self, delivery_tag):
        """Acknowledge the message delivery from RabbitMQ by sending a
        Basic.Ack RPC method for the delivery tag.

        :param int delivery_tag: The delivery tag from the Basic.Deliver frame

        """
        msg = "Acknowledging message {0}"
        msg = msg.format(delivery_tag)
        self._log(msg)

        self._channel.basic_ack(delivery_tag)


    def _stop_consuming(self):
        """Tell RabbitMQ that you would like to stop consuming by sending the
        Basic.Cancel RPC command.

        """
        if self._channel:
            self._log("Sending a Basic.Cancel RPC command to RabbitMQ")
            self._channel.basic_cancel(self._on_cancelok, self._consumer_tag)


    def _on_cancelok(self, unused_frame):
        """This method is invoked by pika when RabbitMQ acknowledges the
        cancellation of a consumer. At this point we will close the channel.
        This will invoke the on_channel_closed method once the channel has been
        closed, which will in-turn close the connection.

        :param pika.frame.Method unused_frame: The Basic.CancelOk frame

        """
        self._log("RabbitMQ acknowledged the cancellation of the consumer")
        self._close_channel()


    def run(self):
        """Run the example consumer by connecting to RabbitMQ and then
        starting the IOLoop to block and allow the SelectConnection to operate.

        """
        self._connection = self._connect()
        self._connection.ioloop.start()


    def stop(self):
        """Cleanly shutdown the connection to RabbitMQ by stopping the consumer
        with RabbitMQ. When RabbitMQ confirms the cancellation, on_cancelok
        will be invoked by pika, which will then closing the channel and
        connection. The IOLoop is started again because this method is invoked
        when CTRL-C is pressed raising a KeyboardInterrupt exception. This
        exception stops the IOLoop which needs to be running for pika to
        communicate with RabbitMQ. All of the commands issued prior to starting
        the IOLoop will be buffered but not processed.

        """
        self._log("Stopping")

        # Stop message consumption.
        self._stop_consuming()

        # Allow keyboard interrupt.
        self._connection.ioloop.start()

        self._log("Stopped")

