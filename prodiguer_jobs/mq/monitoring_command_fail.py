# -*- coding: utf-8 -*-

"""
.. module:: monitoring_command_fail.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring job end messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from prodiguer.db.pgres import dao_monitoring as dao



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      unpack_message_content,
      persist_command,
      )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.command = None
        self.job_uid = None
        self.simulation_uid = None


def unpack_message_content(ctx):
    """Unpacks message being processed.

    """
    ctx.command = ctx.content['command']
    ctx.job_uid = ctx.content['jobuid']
    ctx.simulation_uid = ctx.content['simuid']


def persist_command(ctx):
    """Persists failed command information to db.

    """
    dao.persist_command(
        ctx.simulation_uid,
        ctx.job_uid,
        ctx.msg.uid,
        ctx.msg.timestamp,
        ctx.command,
        True
        )
