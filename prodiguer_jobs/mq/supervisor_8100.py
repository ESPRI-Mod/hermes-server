# -*- coding: utf-8 -*-

"""
.. module:: supervisor_8100.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes supervisor 8100 messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from prodiguer_jobs.mq import utils



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      _unpack_message_content,
      _set_script,
      _format_script,
      _enqueue_supervisor_dispatch
      )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.job_uid = None
        self.simulation_uid = None


def _unpack_message_content(ctx):
    """Unpacks message being processed.

    """
    ctx.job_uid = ctx.content['job_uid']
    ctx.simulation_uid = ctx.content['simulation_uid']


def _set_script(ctx):
    """Sets the script for the job to be executed at the compute node.

    """
    pass


def _format_script(ctx):
    """Formats the script for the job to be executed at the compute node.

    """
    pass


def _enqueue_supervisor_dispatch(ctx):
    """Places a message upon the supervisor dispatch queue.

    """
    utils.enqueue(mq.constants.MESSAGE_TYPE_8200, {
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": unicode(ctx.simulation_uid)
    })
