# -*- coding: utf-8 -*-

"""
.. module:: supervisor_dispatch_script.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Dispatches supervision scripts to HPC for execution.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

from prodiguer import mq
from prodiguer.db import pgres as db
import superviseur



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      _unpack_message_content,
      _set_data,
      _dispatch
      )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.supervision_id = None


def _unpack_message_content(ctx):
    """Unpacks message being processed.

    """
    ctx.supervision_id = int(ctx.content['supervision_id'])


def _set_data(ctx):
    """Sets data to be passed to dispatcher as input.

    """
    ctx.supervision = db.dao_superviseur.retrieve_supervision(ctx.supervision_id)
    ctx.simulation = db.dao_monitoring.retrieve_simulation(ctx.supervision.simulation_uid)
    ctx.job = db.dao_monitoring.retrieve_job(ctx.supervision.job_uid)


def _dispatch(ctx):
    """Dispatches script for execution at HPC.

    """
    # Set dispatch parameters to be passed to dispatcher.
    # TODO verify exactly what information is required.
    params = superviseur.DispatchParameters()

    # Dispatch script to HPC for execution.
    try:
        superviseur.dispatch_script(params)
    # ... handle dispatch errors
    except superviseur.DispatchException as err:
        ctx.supervision.dispatch_error = unicode(err)
    else:
        ctx.supervision.dispatch_error = None
    finally:
        ctx.supervision.dispatch_try_count += 1
        ctx.supervision.dispatch_date = datetime.datetime.utcnow()
        db.session.commit()


def _process_dispatch_error():
    """Processes a dispatch error.

    """
    # Escape if dispatch was successful.
    if ctx.supervision.dispatch_error is None:
        return

    # TODO
    # if dispatch_try_count < N:
    #   requeue with a suitable delay
    # else:
    #    ???
    pass
