# -*- coding: utf-8 -*-

"""
.. module:: simulation_delete.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes messages instructing platform to delete simulation.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
from hermes_jobs.mq import utils
from hermes import mq
from hermes.db import pgres as db
from hermes.db.pgres.dao_monitoring import retrieve_simulation
from hermes.db.pgres.dao_monitoring import retrieve_simulations_by_hashid
from hermes.db.pgres.dao_monitoring import delete_simulation
from hermes.utils import config
from hermes.utils import logger



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _set_simulations,
        _delete,
        _enqueue,
        _log
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True, validate_props=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode, validate_props=validate_props)

        self.is_confirm = False
        self.simulations = []
        self.simulation_uid = None


def _unpack_content(ctx):
    """Unpacks message being processed.

    """
    ctx.simulation_uid = ctx.content['simuid']
    ctx.is_confirm = ctx.content.get('is_confirm') is not None


def _set_simulations(ctx):
    """Sets simulations to be deleted.

    """
    simulation = retrieve_simulation(ctx.simulation_uid)
    if simulation:
        ctx.simulations = retrieve_simulations_by_hashid(simulation.hashid)


def _delete(ctx):
    """Deletes simulation data from dB.

    """
    if ctx.simulations:
        for s in ctx.simulations:
            delete_simulation(s.uid)
        db.session.commit()


def _enqueue(ctx):
    """Ensure simulation is purged when email latency interferes with monitoring.

    """
    if ctx.is_confirm:
        return

    utils.enqueue(
        mq.constants.MESSAGE_TYPE_8888,
        delay_in_ms=config.apps.monitoring.purgeSimulationConfirmDelayInSeconds * 1000,
        exchange=mq.constants.EXCHANGE_HERMES_SECONDARY_DELAYED,
        payload={"simuid": ctx.simulation_uid, "is_confirm": True}
        )


def _log(ctx):
    """Logs event.

    """
    if ctx.is_confirm:
        logger.log_mq("Simulation purge confirmed: {}".format(ctx.simulation_uid))
    else:
        logger.log_mq("Simulation purged: {}".format(ctx.simulation_uid))
