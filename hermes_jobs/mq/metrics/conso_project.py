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
import os
import time

from sqlalchemy.exc import IntegrityError

from hermes import mq
from hermes.db import pgres as db
from hermes.db.pgres import dao_conso as dao
from hermes.utils import config
from hermes.utils import logger
from hermes_jobs.mq.utils import enqueue
from hermes_jobs.mq.metrics import conso_cpt_parser as parser



# Supported HPC's.
_HPC_TGCC = 'tgcc'
_HPC_IDRIS = 'idris'


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      _unpack,
      _verify_write_cpt_file,
      _write_cpt_file,
      _set_blocks,
      _set_block_allocation,
      _persist_block_total,
      _persist_block_subtotals,
      _persist_block_logins
      )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True, validate_props=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(props, body, decode=decode, validate_props=validate_props)

        self.project = None
        self.blocks = []
        self.centre = None
        self.data = None
        self.output_dir = config.apps.conso.outputDirectory

        now = dt.datetime.utcnow()
        self.date = now.isoformat()[0:10]
        self.timestamp = int(time.mktime(now.timetuple()))


def _unpack(ctx):
    """Unpacks message content.

    """
    ctx.cpt = base64.decodestring(ctx.content['data'])
    ctx.centre = ctx.content['centre'].lower()
    ctx.project = ctx.content['accountingProject'].lower()


def _verify_write_cpt_file(ctx):
    """Verifies whether writing the cpt file to the file system is required or not.

    """
    # CPT file is empty.
    if not ctx.cpt:
        logger.log_mq_warning("CONSO: empty cpt file -> {} :: {}".format(ctx.centre, ctx.project))
        ctx.abort = True


def _write_cpt_file(ctx):
    """Writes cpt file to file system.

    """
    # Set file path.
    fpath = ctx.output_dir
    fpath = os.path.join(fpath, ctx.centre)
    fpath = os.path.join(fpath, ctx.project)
    fpath = os.path.join(fpath, ctx.date)
    if not os.path.exists(fpath):
        os.makedirs(fpath)
    fpath = os.path.join(fpath, "{}.txt".format(ctx.timestamp))

    # Write to file system.
    with open(fpath, 'w') as fstream:
        fstream.write(ctx.cpt)

    # Log.
    logger.log_mq("CONSO - cpt file written to fs -> {} :: {}".format(ctx.centre, ctx.project))


def _set_blocks(ctx):
    """Unpacks conso blocks from cpt data.

    """
    ctx.blocks = parser.get_blocks(ctx.centre, ctx.cpt, ctx.project)


def _log_block_warning(ctx, msg, block):
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
    _log_block_warning(ctx, "allocation not found", block)

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
    # Log.
    _log_block_warning(ctx, "allocation is inactive", block)

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
        if not block['allocation'].is_active:
            _on_block_allocation_inactive(ctx, block)


def _persist_block_total(ctx):
    """Persists block total information to db.

    """
    for block in ctx.blocks:
        block['header'] = None
        try:
            block['header'] = dao.persist_consumption(
                block['allocation'].id,
                block['consumption_date'],
                block['project_total']
                )
        except IntegrityError:
            db.session.rollback()
            block['header'] = dao.retrieve_consumption_header(
                block['allocation'].id,
                block['consumption_date']
                )


def _persist_block_subtotals(ctx):
    """Persists block sub-total information to db.

    """
    for block in [b for b in ctx.blocks if b['header']]:
        for sub_project, sub_total in block['subtotals']:
            try:
                dao.persist_consumption(
                    block['allocation'].id,
                    block['consumption_date'],
                    sub_total,
                    sub_project=sub_project,
                    batch_date=block['header'].row_create_date
                    )
            except IntegrityError:
                db.session.rollback()


def _persist_block_logins(ctx):
    """Persists block login information to db.

    """
    for block in [b for b in ctx.blocks if b['header']]:
        for login, sub_project, total_hours in block['consumption']:
            try:
                dao.persist_consumption(
                    block['allocation'].id,
                    block['consumption_date'],
                    total_hours,
                    login=login,
                    sub_project=sub_project,
                    batch_date=block['header'].row_create_date
                    )
            except IntegrityError:
                db.session.rollback()
