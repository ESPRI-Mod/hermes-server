# -*- coding: utf-8 -*-

"""
.. module:: monitoring_job_update.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring job update messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
from hermes import mq
from hermes.db.pgres import dao_monitoring as dao
from hermes_jobs.mq import utils as mq_utils



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack,
        _persist,
        _enqueue
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True, validate_props=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(props, body, decode=decode, validate_props=validate_props)

        self.job_uid = None
        self.period = None
        self.period_date_begin = None
        self.period_date_end = None
        self.period_ordinal = None
        self.simulation_uid = None


def _unpack(ctx):
    """Unpacks message being processed.

    """
    ctx.simulation_uid = ctx.content['simuid']
    ctx.job_uid = ctx.content['jobuid']
    ctx.period_ordinal = int(ctx.content['CumulPeriod'] or 0)
    ctx.period_date_begin = int(ctx.content['PeriodDateBegin'])
    ctx.period_date_end = int(ctx.content['PeriodDateEnd'])


def _persist(ctx):
    """Persists job updates to dB.

    """
    ctx.period = dao.persist_job_period(
        ctx.simulation_uid,
        ctx.job_uid,
        ctx.period_ordinal,
        ctx.period_date_begin,
        ctx.period_date_end
        )


def _enqueue(ctx):
    """Places a message upon the front-end notification queue.

    """
    mq_utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": u"job_period_update",
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": ctx.simulation_uid,
        "period_ordinal": ctx.period_ordinal,
        "period_date_begin": ctx.period_date_begin,
        "period_date_end": ctx.period_date_end
    })
