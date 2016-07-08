# -*- coding: utf-8 -*-

"""
.. module:: monitoring_job_update.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring job update messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
from prodiguer import mq
from prodiguer.db.pgres import dao_monitoring as dao
from hermes_jobs.mq import utils as mq_utils



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _persist
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.job_uid = None
        self.period_date_begin = None
        self.period_date_end = None
        self.period_id = None
        self.simulation_uid = None


def _unpack_content(ctx):
    """Unpacks message being processed.

    """
    ctx.simulation_uid = ctx.content['simuid']
    ctx.job_uid = ctx.content['jobuid']
    ctx.period_id = ctx.content['CumulPeriod']
    ctx.period_date_begin = ctx.content['PeriodDateBegin']
    ctx.period_date_end = ctx.content['PeriodDateEnd']


def _persist(ctx):
    """Persists job updates to dB.

    """

    dao.persist_job_period(
        ctx.simulation_uid,
        ctx.job_uid,
        int(ctx.period_id or 0),
        int(ctx.period_date_begin),
        int(ctx.period_date_end)
        )
