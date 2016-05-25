# -*- coding: utf-8 -*-

"""
.. module:: supervisor_format_script.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Formats supervision scripts in readiness for dispatch.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from prodiguer.db import pgres as db
from prodiguer.utils import logger
from hermes_jobs.mq import utils
import superviseur



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _set_data,
        _authorize,
        _format,
        _enqueue_script_dispatch,
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
        self.script = None
        self.user = None


def _unpack_content(ctx):
    """Unpacks message being processed.

    """
    ctx.supervision_id = int(ctx.content['supervision_id'])


def _set_data(ctx):
    """Sets data to be passed to script formatter as input.

    """
    ctx.supervision = db.dao_superviseur.retrieve_supervision(ctx.supervision_id)
    ctx.simulation = db.dao_monitoring.retrieve_simulation(ctx.supervision.simulation_uid)
    ctx.job = db.dao_monitoring.retrieve_job(ctx.supervision.job_uid)


def _authorize(ctx):
    """Verifies that the user has authorized supervision.

    """
    try:
        ctx.user = superviseur.authorize(ctx.simulation.compute_node_login)
    except UserWarning as err:
        logger.log_mq_warning("Supervision dispatch unauthorized: {}".format(err))
        ctx.abort = True


def _format(ctx):
    """Formats the script for the job to be executed at the compute node.

    """
    # Set dispatch parameters to be passed to dispatcher.
    params = superviseur.FormatParameters(
        ctx.simulation, ctx.job, ctx.supervision, ctx.user)

    # Format script to be dispatched to HPC for execution.
    try:
        ctx.supervision.script = superviseur.format_script(params)
    # ... handle formatting errors
    # TODO define error strategy
    except:
        ctx.abort = True
    else:
        db.session.commit()


def _enqueue_script_dispatch(ctx):
    """Enqueues a script dispatch messages.

    """
    utils.enqueue(mq.constants.MESSAGE_TYPE_8200, {
        "supervision_id": ctx.supervision_id
    })
