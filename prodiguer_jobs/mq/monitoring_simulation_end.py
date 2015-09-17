# -*- coding: utf-8 -*-

"""
.. module:: monitoring_simulation_end.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring 0100 messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
from prodiguer import mq
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer_jobs.mq import monitoring_job_end
from prodiguer_jobs.mq import utils



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        monitoring_job_end.unpack_message_content,
        _persist_simulation,
        monitoring_job_end.persist_job,
        _enqueue_front_end_notification
    )


class ProcessingContextInfo(monitoring_job_end.ProcessingContextInfo):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.simulation = None


def _persist_simulation(ctx):
    """Persists simulation updates to dB.

    """
    ctx.simulation = dao.persist_simulation_02(
        ctx.msg.timestamp,
        ctx.props.type == mq.constants.MESSAGE_TYPE_9999,
        ctx.simulation_uid
        )


def _enqueue_front_end_notification(ctx):
    """Places a message upon the front-end notification queue.

    """
    # Skip if the 0000 message has not yet been received.
    if ctx.simulation.hashid is None:
        return

    # Skip if not the active simulation.
    active_simulation = dao.retrieve_active_simulation(ctx.simulation.hashid)
    if ctx.simulation.uid != active_simulation.uid:
        return
        
    # Set front-end event type.    
    if ctx.props.type == mq.constants.MESSAGE_TYPE_9999:
        event_type = u"simulation_error"
    else:
        event_type = u"simulation_complete"

    utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": event_type,
        "simulation_uid": unicode(ctx.simulation_uid)
    })
