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



# Supported HPC's.
_HPC_TGCC = 'tgcc'
_HPC_IDRIS = 'idris'


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      _unpack_content,
      _reformat_raw_metrics,
      _unpack_blocks_tgcc,
      _unpack_blocks_idris,
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


def _reformat_raw_metrics(ctx):
    """Reformats raw metrics prior to further processing.

    """
    ctx.cpt = [l.strip().lower() for l in ctx.cpt.split('\n') if l.strip()]


def _unpack_blocks_idris(ctx):
    """Unpacks blocks of IDRIS HPC conso metrics from data.

    """
    # Escape if not processing IDRIS metrics.
    if ctx.centre != _HPC_IDRIS:
        return

    # Set metric block start end positions.
    indexes = zip(
        [i for i, v in enumerate(ctx.cpt) if v.find('mise a jour') > -1],
        [i for i, v in enumerate(ctx.cpt) if v.find('totaux') > -1 and
                                              len(v.split()) == 4]
        )

    # Set blocks of metrics to be persisted.
    for start, end in indexes:
        ctx.blocks.append({
            'project': ctx.project,
            'sub_project': None,
            'machine': ctx.cpt[start + 1].split()[-1][:-1],
            'node': 'standard',
            'consumption_date': dt.datetime.strptime(
                "{} {}".format(ctx.cpt[start].split()[-2:][0],
                               ctx.cpt[start].split()[-2:][1]),
                               "%d/%m/%Y %H:%M"
            ),
            'consumption': [(l[-5], float() if l[-3] == '-' else float(l[-3]))
                            for l in [l.split() for l in ctx.cpt[start + 7: end - 1]]],
            'total': float(ctx.cpt[end].split()[1]),
            'project_allocation': float(ctx.cpt[start + 3].split()[-1]),
            'project_end_date': dt.datetime(ctx.year, 12, 31, 23, 59, 59),
            'project_start_date': dt.datetime(ctx.year, 01, 01)
            })


def _unpack_blocks_tgcc(ctx):
    """Unpacks blocks of TGCC HPC conso metrics from data.

    """
    # Escape if not processing TGCC metrics.
    if ctx.centre != _HPC_TGCC:
        return

    # Set metric block start end positions.
    indexes = zip(
        [i for i, v in enumerate(ctx.cpt) if v.startswith('accounting')],
        [i for i, v in enumerate(ctx.cpt) if v.startswith('project')]
        )

    # Set blocks of metrics to be persisted.
    for start, end in indexes:
        ctx.blocks.append({
            'allocation': None,
            'project': ctx.cpt[start].split()[3].lower(),
            'sub_project': None,
            'machine': ctx.cpt[start].split()[5].lower(),
            'node': ctx.cpt[start].split()[6].lower(),
            'consumption_date': dt.datetime.strptime(
                "{} 23:59:59".format(ctx.cpt[start].split()[-1]), "%Y-%m-%d %H:%M:%S"),
            'consumption': [(n, float(t)) for n, t in
                            [l.split() for l in ctx.cpt[start + 2: end - 4]]],
            'total': float(ctx.cpt[end - 4].split()[-1]),
            'project_allocation': float(ctx.cpt[end - 3].split()[-1]),
            'project_end_date': dt.datetime.strptime(ctx.cpt[end].split()[-1], "%Y-%m-%d"),
            'project_start_date': dt.datetime(ctx.year, 01, 01)
            })


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
        True,       # is_active
        False       # is_reviewed
        )

    # Alert operator.
    enqueue(mq.constants.MESSAGE_TYPE_ALERT, {
        "trigger": u"conso-new-allocation",
        "allocation_id": block['allocation'].id
        })


def _on_block_allocation_inactive(ctx, block)):
    """Event handler invoked when a block allocation is inactive.

    """
    # TODO: escape if date is not 01/01 ?

    # Log.
    _log_block_warning("allocation is inactive", block)

    # Alert operator.
    enqueue(mq.constants.MESSAGE_TYPE_ALERT, {
        "trigger": u"conso-inactive-allocation",
        "allocation_id": block['allocation'].id
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
        project_conso = dao.persist_consumption(
            block['allocation'].id
            block['consumption_date'],
            block['total']
            )

        # Persist login consumptions.
        for login, total_hours in block['consumption']:
            dao.persist_consumption(
                block['allocation'].id
                block['consumption_date'],
                total_hours,
                login=login,
                batch_date=project_conso.row_create_date
                )
