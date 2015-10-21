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
from prodiguer_jobs.mq import utils
import superviseur



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _set_data,
        _format
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


def _format(ctx):
    """Formats the script for the job to be executed at the compute node.

    """
    # Set dispatch parameters to be passed to dispatcher.
    # TODO verify exactly what information is required.
    params = superviseur.FormatParameters()

    # Format script to be dispatched to HPC for execution.
    try:
        ctx.supervision.script = superviseur.format_script(params)
    # ... handle formatting errors
    except superviseur.FormatException as err:
        # TODO define error strategy
        pass
    else:
        db.session.commit()
        utils.enqueue(mq.constants.MESSAGE_TYPE_8200, {
            "supervision_id": ctx.supervision_id
        })
