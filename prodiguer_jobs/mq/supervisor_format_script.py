# -*- coding: utf-8 -*-

"""
.. module:: supervisor_format_script.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Formats supervision scripts in readiness for dispatch.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from prodiguer.db import pgres as db
from prodiguer.utils import logger
from prodiguer_jobs.mq import utils



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_message_content,
        _set_data,
        _format_script,
        _persist_script,
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

        self.job = None
        self.simulation = None
        self.supervision = None
        self.supervision_id = None


def _unpack_message_content(ctx):
    """Unpacks message being processed.

    """
    ctx.supervision_id = int(ctx.content['supervision_id'])


def _set_data(ctx):
    """Sets data to be passed to script formatter as input.

    """
    ctx.supervision = db.dao_superviseur.retrieve_supervision(ctx.supervision_id)
    ctx.simulation = db.dao_monitoring.retrieve_simulation(ctx.supervision.simulation_uid)
    ctx.job = db.dao_monitoring.retrieve_job(ctx.supervision.job_uid)

    print ctx.supervision
    print ctx.simulation
    print ctx.job


def _format_script(ctx):
    """Formats the script for the job to be executed at the compute node.

    """
    # TODO pass context to prodiguer-superviseur.format function
    pass


def _persist_script(ctx):
    """Persists supervision script to database.

    """
    # TODO update database table
    pass


def _enqueue_supervisor_dispatch(ctx):
    """Places a message upon the supervisor dispatch queue.

    """
    logger.log_mq("Dispathcing 8200")
    utils.enqueue(mq.constants.MESSAGE_TYPE_8200, {
        "supervision_id": ctx.supervision_id
    })
