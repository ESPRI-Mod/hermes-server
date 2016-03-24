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
      _unpack_metrics_tgcc,
      _unpack_metrics_idris,
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

        self.accounting_project = None
        self.metrics = []
        self.centre = None
        self.data = None


def _unpack_content(ctx):
    """Unpacks message content.

    """
    ctx.data = base64.decodestring(ctx.content['data'])
    ctx.centre = ctx.content['centre']
    ctx.accounting_project = ctx.content['accountingProject']


def _reformat_raw_metrics(ctx):
    """Reformats raw metrics prior to further processing.

    """
    ctx.data = [l.lower() for l in ctx.data.split('/n') if l]


def _unpack_metrics_idris(ctx):
    """Unpacks IDRIS HPC conso metrics from data.

    """
    # Escape if not processing IDRIS metrics.
    if ctx.centre.lower() != _HPC_IDRIS:
        return

    # TODO


def _unpack_metrics_tgcc(ctx):
    """Unpacks TGCC HPC conso metrics from data.

    """
    # Escape if not processing TGCC metrics.
    if ctx.centre.lower() != _HPC_TGCC:
        return

    # Set metric block start end positions.
    indexes = zip(
        [i for i, v in enumerate(ctx.data) if v.startswith('accounting')],
        [i for i, v in enumerate(ctx.data) if v.startswith('project')]
        )

    # Set metrics to be persisted.
    for start, end in indexes:
        ctx.metrics.append({
            'project': ctx.data[start].split()[3].lower(),
            'machine': ctx.data[start].split()[5].lower(),
            'node': ctx.data[start].split()[6].lower(),
            'compute_date': dt.datetime.strptime(
                "{} 23:59:59".format(ctx.data[start].split()[-1]), "%Y-%m-%d %H:%M:%S"),
            'logins': [(n, float(t)) for n, t in [l.split() for l in ctx.data[start + 2: end - 4]]],
            'total': float(ctx.data[end - 4].split()[-1]),
            'allocated': float(ctx.data[end - 3].split()[-1]),
            'project_end_date': dt.datetime.strptime(ctx.data[end].split()[-1], "%Y-%m-%d"),
            })


def _persist(ctx):
    """Persists information to db.

    """
    for metric in ctx.metric:
        print metric

    logger.log_mq("TODO persist conso project metrics")

    # Get allocation id.
    pass

    # Insert login metrics.
    pass
