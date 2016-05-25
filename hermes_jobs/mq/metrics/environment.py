# -*- coding: utf-8 -*-

"""
.. module:: environment.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring 7000 messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db import pgres as db



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return _persist


def _persist(ctx):
    """Persists information to db.

    """
    db.dao_monitoring.persist_environment_metric(
        ctx.content['actionName'],
        ctx.msg.timestamp,
        ctx.content['dirFrom'],
        ctx.content['dirTo'],
        ctx.content['duration_ms'],
        ctx.content['jobuid'],
        ctx.content['simuid'],
        ctx.content['size_Mo'],
        ctx.content['throughput_Mo_s']
        )
