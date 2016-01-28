# -*- coding: utf-8 -*-

"""
.. module:: internal_alert.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Dispatches platform alerts via email/sms ... etc.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from prodiguer.utils import logger



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _set_handler
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.trigger = None
        self.handler = None


def _unpack_content(ctx):
    """Unpacks message content.

    """
    ctx.trigger = ctx.content['trigger']


def _set_handler(ctx):
    """Assign an alert handler.

    """
    logger.log_mq("TODO: dispatch {} alert".format(ctx.trigger))
