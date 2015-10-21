# -*- coding: utf-8 -*-

"""
.. module:: consumer_utils.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Message consumer utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os
import subprocess

import arrow

from prodiguer import mq
from prodiguer import __version__ as PRODIGUER_VERSION



def enqueue(
    message_type,
    payload=None,
    user_id=mq.constants.USER_PRODIGUER,
    producer_id = mq.constants.PRODUCER_PRODIGUER,
    producer_version = PRODIGUER_VERSION,
    exchange=mq.constants.EXCHANGE_PRODIGUER_SECONDARY,
    delay_in_ms=None
    ):
    """Enqueues a message upon MQ server.

    :param str message_type: Message type, e.g. 0000.
    :param dict payload: Message payload.
    :param str user_id: MQ user id, e.g. prodiguer-mq-user.
    :param str producer_id: MQ producer identifier, e.g. libIGCM.
    :param str producer_version: MQ server producer version, e.g. 2.7.
    :param str exchange: MQ exchange, e.g. x-secondary.
    :param int delay_in_ms: Delay (in milliseconds) before message is routed.

    """
    def _get_msg_props():
        """Returns AMPQ message properties.

        """
        return mq.utils.create_ampq_message_properties(
            user_id=user_id,
            producer_id=producer_id,
            producer_version=producer_version,
            message_type=message_type,
            delay_in_ms=delay_in_ms
            )

    def _yield_message():
        """Yeild a mesage to be enqueued.

        """
        yield mq.Message(_get_msg_props(), payload or {})

    mq.produce(_yield_message)


def get_timestamp(timestamp):
    """Corrects nano-second to micro-second precision and returns updated timestamp.

    :param str timestamp: Incoming message timestamp.

    :return: Formatted micro-second precise UTC timestamp.
    :rtype: datetime.datetime

    """
    try:
        return arrow.get(timestamp).to('UTC').datetime
    except arrow.parser.ParserError:
        part1 = timestamp.split(".")[0]
        part2 = timestamp.split(".")[1].split("+")[0][0:6]
        part3 = timestamp.split(".")[1].split("+")[1]
        timestamp = "{0}.{1}+{2}".format(part1, part2, part3)

        return arrow.get(timestamp).to('UTC').datetime


def exec_shell_command(cmd):
    """Executes a prodiguer-shell command.

    :param str cmd: Prodiguer shell command to be executed.

    """
    cmd_type = cmd.split("-")[0]
    cmd_name = "_".join(cmd.split("-")[1:])
    script = os.getenv("PRODIGUER_HOME")
    script = os.path.join(script, "bash")
    script = os.path.join(script, cmd_type)
    script = os.path.join(script, "{}.sh".format(cmd_name))
    subprocess.call(script, shell=True)
