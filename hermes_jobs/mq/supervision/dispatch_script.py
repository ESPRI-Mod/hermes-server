# -*- coding: utf-8 -*-

"""
.. module:: supervisor_dispatch_script.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Dispatches supervision scripts to HPC for execution.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

from hermes import mq
from hermes.db import pgres as db
from hermes.db.pgres.dao_monitoring import retrieve_job
from hermes.db.pgres.dao_monitoring import retrieve_latest_job_period
from hermes.db.pgres.dao_monitoring import retrieve_latest_job_period_counter
from hermes.db.pgres.dao_monitoring import retrieve_simulation
from hermes.db.pgres.dao_superviseur import retrieve_supervision
from hermes.utils import logger
import superviseur



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      _unpack,
      _set_data_01,
      _authorize,
      _set_data_02,
      _dispatch,
      _process_dispatch_error
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
        self.job_period = None
        self.simulation = None
        self.supervision = None
        self.supervision_id = None
        self.user = None


def _unpack(ctx):
    """Unpacks message being processed.

    """
    ctx.supervision_id = int(ctx.content['supervision_id'])


def _set_data_01(ctx):
    """Sets data to be passed to dispatcher as input.

    """
    ctx.supervision = retrieve_supervision(ctx.supervision_id)
    ctx.simulation = retrieve_simulation(ctx.supervision.simulation_uid)


def _authorize(ctx):
    """Verifies that the user has authorized supervision.

    """
    try:
        ctx.user = superviseur.authorize(ctx.simulation.compute_node_login)
    except UserWarning as err:
        logger.log_mq_warning("Supervision unauthorized: {}".format(err))
        ctx.abort = True


def _set_data_02(ctx):
    """Sets data to be passed to dispatcher as input.

    """
    ctx.job = retrieve_job(ctx.supervision.job_uid)
    ctx.job_period = retrieve_latest_job_period(ctx.supervision.simulation_uid)
    ctx.job_period_counter = retrieve_latest_job_period_counter(ctx.supervision.simulation_uid)


def _dispatch(ctx):
    """Dispatches script for execution at HPC.

    """
    # Set dispatch parameters to be passed to dispatcher.
    params = superviseur.DispatchParameters(
        ctx.simulation,
        ctx.job,
        ctx.job_period,
        ctx.job_period_counter,
        ctx.supervision,
        ctx.user
        )

    # Dispatch script to HPC for execution.
    try:
        superviseur.dispatch_script(params)
    # ... handle dispatch errors
    except Exception as err:
        ctx.supervision.dispatch_error = unicode(err)
    else:
        ctx.supervision.dispatch_error = None
    finally:
        ctx.supervision.dispatch_try_count += 1
        ctx.supervision.dispatch_date = datetime.datetime.utcnow()
        db.session.commit()


def _process_dispatch_error(ctx):
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
