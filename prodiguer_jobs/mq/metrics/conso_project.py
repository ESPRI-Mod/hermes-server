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
    ctx.data = base64.decodestring(ctx.content['data'])
    ctx.centre = ctx.content['centre'].lower()
    ctx.project = ctx.content['accountingProject'].lower()


def _reformat_raw_metrics(ctx):
    """Reformats raw metrics prior to further processing.

    """
    ctx.data = [l.strip().lower() for l in ctx.data.split('\n') if l.strip()]


def _unpack_blocks_idris(ctx):
    """Unpacks blocks of IDRIS HPC conso metrics from data.

    """
    # Escape if not processing IDRIS metrics.
    if ctx.centre != _HPC_IDRIS:
        return

    # Set metric block start end positions.
    indexes = zip(
        [i for i, v in enumerate(ctx.data) if v.find('mise a jour') > -1],
        [i for i, v in enumerate(ctx.data) if v.find('totaux') > -1 and
                                              len(v.split()) == 4]
        )

    # Set blocks of metrics to be persisted.
    for start, end in indexes:
        ctx.blocks.append({
            # 'sub_project': ctx.data[start].split()[3].lower(),
            'sub_project': None,
            'machine': ctx.data[start + 1].split()[-1][:-1],
            'node': 'standard',
            'consumption_date': dt.datetime.strptime(
                "{} {}".format(ctx.data[start].split()[-2:][0],
                               ctx.data[start].split()[-2:][1]),
                               "%d/%m/%Y %H:%M"
            ),
            'consumption': [(l[-5], float() if l[-3] == '-' else float(l[-3]))
                            for l in [l.split() for l in ctx.data[start + 7: end - 1]]],
            'total': float(ctx.data[end].split()[1]),
            'allocated': float(ctx.data[start + 3].split()[-1]),
            'project_end_date': None,
            })


def _unpack_blocks_tgcc(ctx):
    """Unpacks blocks of TGCC HPC conso metrics from data.

    """
    # Escape if not processing TGCC metrics.
    if ctx.centre != _HPC_TGCC:
        return

    # Set metric block start end positions.
    indexes = zip(
        [i for i, v in enumerate(ctx.data) if v.startswith('accounting')],
        [i for i, v in enumerate(ctx.data) if v.startswith('project')]
        )

    # Set blocks of metrics to be persisted.
    for start, end in indexes:
        ctx.blocks.append({
            'sub_project': ctx.data[start].split()[3].lower(),
            'machine': ctx.data[start].split()[5].lower(),
            'node': ctx.data[start].split()[6].lower(),
            'consumption_date': dt.datetime.strptime(
                "{} 23:59:59".format(ctx.data[start].split()[-1]), "%Y-%m-%d %H:%M:%S"),
            'consumption': [(n, float(t)) for n, t in
                            [l.split() for l in ctx.data[start + 2: end - 4]]],
            'total': float(ctx.data[end - 4].split()[-1]),
            'allocated': float(ctx.data[end - 3].split()[-1]),
            'project_end_date': dt.datetime.strptime(ctx.data[end].split()[-1], "%Y-%m-%d"),
            })


def _persist(ctx):
    """Persists information to db.

    """
    for block in ctx.blocks:
        # Get related allocation.
        allocation = dao.retrieve_allocation(
            ctx.centre,
            ctx.project,
            block['machine'],
            block['node'],
            block['consumption_date']
            )

        # Skip blocks that cannot be mapped to an allocation.
        if allocation is None:
            logger.log_mq_warning("Conso metrics block could not be mapped to an allocation")
            continue

        # Persist batch total.
        batch = dao.persist_consumption(
            allocation.id,
            block['consumption_date'],
            block['total']
            )

        # Persist batch items.
        for login, total_hours in block['consumption']:
            dao.persist_consumption(
                allocation.id,
                block['consumption_date'],
                total_hours,
                login=login,
                batch_date=batch.row_create_date
                )
