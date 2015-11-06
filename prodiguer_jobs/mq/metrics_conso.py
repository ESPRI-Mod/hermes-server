# -*- coding: utf-8 -*-

"""
.. module:: metrics_conso.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring 7100 messages (resource consumption metrics).

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres import dao_conso as dao



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return _persist


def _persist(ctx):
    """Persists information to db.

    """
    # Persist allocation info.
    allocation = dao.persist_allocation(
        ctx.content['projectCentre'],
        ctx.content['projectEndDate'],
        ctx.content['projectMachine'],
        ctx.content['projectNodeType'],
        ctx.content['projectName'],
        ctx.content['projectStartDate'],
        ctx.content['projectAllocation']
        )

    # Persist occupation store info.
    dao.persist_occupation_store(
        ctx.content['occupationStoreDate'],
        ctx.content['occupationStoreLogin'],
        ctx.content['occupationStoreName'],
        ctx.content['occupationStoreSize']
        )

    # Persist consumption by login info.
    dao.persist_consumption(
        allocation.id,
        ctx.content['consumptionByLoginDate'],
        ctx.content['consumptionByLoginLogin'],
        ctx.content['consumptionByLoginTotal']
        )

    # Persist consumption by project info.
    dao.persist_consumption(
        allocation.id,
        ctx.content['consumptionByProjectDate'],
        ctx.content['consumptionByProjectTotal']
        )
