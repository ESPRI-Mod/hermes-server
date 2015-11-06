# -*- coding: utf-8 -*-

"""
.. module:: monitoring_command_fail.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring job end messages.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres import dao_monitoring as dao



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return _persist


def _persist(ctx):
    """Persists information to db.

    """
    dao.persist_command(
        ctx.content['simuid'],
        ctx.content['jobuid'],
        ctx.msg.uid,
        ctx.msg.timestamp,
        ctx.content['command'],
        True
        )
