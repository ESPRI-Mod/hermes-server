# -*- coding: utf-8 -*-

"""
.. module:: conso_project.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring 7010 messages (resource consumption metrics).

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import base64
import datetime as dt

from prodiguer import mq
from prodiguer.db.pgres import dao_conso as dao
from prodiguer.utils import logger
from prodiguer_jobs.mq.utils import enqueue
from prodiguer_jobs.mq.metrics import conso_cpt_parser as parser



# Supported HPC's.
_HPC_TGCC = 'tgcc'
_HPC_IDRIS = 'idris'


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      _unpack_content,
      _set_blocks,
      _set_block_allocation,
      _persist,
      )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.project = None
        self.blocks = []
        self.centre = None
        self.data = None


def _unpack_content(ctx):
    """Unpacks message content.

    """
    ctx.cpt = base64.decodestring(ctx.content['data'])
    ctx.centre = ctx.content['centre'].lower()
    ctx.project = ctx.content['accountingProject'].lower()
    ctx.year = dt.datetime.utcnow().year


def _set_blocks(ctx):
    """Unpacks conso blocks from cpt data.

    """
    ctx.blocks = parser.get_blocks(ctx.centre, ctx.cpt, ctx.project)


def _log_block_warning(msg, block):
    """Write a block warning to log file.

    """
    msg = "CONSO: block {}: {} :: {} :: {} :: {} :: {}".format(
        msg,
        ctx.centre,
        block['project'],
        block['machine'],
        block['node'],
        block['consumption_date']
        )
    logger.log_mq_warning(msg)


def _on_block_allocation_not_found(ctx, block):
    """Event handler invoked when a block allocation is not found.

    """
    # Log.
    _log_block_warning("allocation not found", block)

    # Persist.
    block['allocation'] = dao.persist_allocation(
        ctx.centre,
        block['project'],
        None,  # sub-project
        block['machine'],
        block['node'],
        block['project_start_date'],
        block['project_end_date'],
        block['project_allocation'],
        True,
        False
        )

    # Alert operator.
    enqueue(mq.constants.MESSAGE_TYPE_ALERT, {
        "trigger": u"conso-new-allocation",
        "allocation_id": block['allocation'].id,
        "centre": ctx.centre
        })


def _on_block_allocation_inactive(ctx, block):
    """Event handler invoked when a block allocation is inactive.

    """
    # TODO: escape if date is not 01/01 ?

    # Log.
    _log_block_warning("allocation is inactive", block)

    # Alert operator.
    enqueue(mq.constants.MESSAGE_TYPE_ALERT, {
        "trigger": u"conso-inactive-allocation",
        "allocation_id": block['allocation'].id,
        "centre": ctx.centre
        })


def _set_block_allocation(ctx):
    """Assigns an allocation to each block.

    """
    for block in ctx.blocks:
        # Get block allocation.
        block['allocation'] = dao.retrieve_allocation(
            ctx.centre,
            block['project'],
            block['machine'],
            block['node'],
            block['consumption_date']
            )

        # Block allocation not found.
        if block['allocation'] is None:
            _on_block_allocation_not_found(ctx, block)

        # Block allocation is inactive.
        if block['allocation'].status == 'inactive':
            _on_block_allocation_inactive(ctx, block)


def _persist(ctx):
    """Persists information to db.

    """
    for block in ctx.blocks:
        # Persist project consumption.
        header = dao.persist_consumption(
            block['allocation'].id,
            block['consumption_date'],
            block['total']
            )

        # Persist login consumptions.
        for login, total_hours in block['consumption']:
            dao.persist_consumption(
                block['allocation'].id,
                block['consumption_date'],
                total_hours,
                login=login,
                batch_date=header.row_create_date
                )
