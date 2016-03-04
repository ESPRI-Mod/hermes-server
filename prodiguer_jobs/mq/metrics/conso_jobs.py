# -*- coding: utf-8 -*-

"""
.. module:: conso_job_status.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring 7011 messages (job status metrics).

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
    logger.log_mq("TODO persist conso job status metrics")
    return

