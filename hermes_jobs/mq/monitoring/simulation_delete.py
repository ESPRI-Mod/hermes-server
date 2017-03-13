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
from prodiguer import mq
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer.utils import config
from prodiguer.utils import logger



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _delete,
        _enqueue,
        _log
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.simulation_uid = None


def _unpack_content(ctx):
    """Unpacks message being processed.

    """
    ctx.simulation_uid = ctx.content['simuid']
    ctx.is_confirm = ctx.content.get('is_confirm') is not None


def _delete(ctx):
    """Deletes simulation data from dB.

    """
    dao.delete_simulation(ctx.simulation_uid)


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
    logger.log_mq("Simulation purged: {}".format(ctx.simulation_uid))
