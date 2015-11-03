# -*- coding: utf-8 -*-

"""
.. module:: metrics_conso.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring 7100 messages (resource consumption metrics).

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from prodiguer.db import pgres as db
from prodiguer.db import dao_conso as dao



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      _unpack_content,
      _persist_metric
      )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.consumption_by_login_date = None
        self.consumption_by_login_login = None
        self.consumption_by_login_total = None
        self.consumption_by_project_date = None
        self.consumption_by_project_total = None
        self.job_uid = None
        self.occupation_store_date = None
        self.occupation_store_login = None
        self.occupation_store_name = None
        self.occupation_store_size = None
        self.project = None
        self.project_allocation_duration = None
        self.project_allocation_end_date = None
        self.project_allocation_start_date = None
        self.project_centre = None
        self.project_machine = None
        self.project_name = None
        self.project_node_type = None
        self.simulation_uid = None


def _unpack_content(ctx):
    """Unpacks message being processed.

    """
    ctx.consumption_by_login_date = ctx.content['consumptionByLoginDate']
    ctx.consumption_by_login_login = ctx.content['consumptionByLoginLogin']
    ctx.consumption_by_login_total = ctx.content['consumptionByLoginTotal']

    ctx.consumption_by_project_date = ctx.content['consumptionByProjectDate']
    ctx.consumption_by_project_total = ctx.content['consumptionByProjectTotal']

    ctx.job_uid = ctx.content['jobuid']
    ctx.occupation_store_date = ctx.content['occupationStoreDate']
    ctx.occupation_store_login = ctx.content['occupationStoreLogin']
    ctx.occupation_store_name = ctx.content['occupationStoreName']
    ctx.occupation_store_size = ctx.content['occupationStoreSize']
    ctx.project_allocation_duration = ctx.content['projectAllocation']
    ctx.project_allocation_end_date = ctx.content['projectEndDate ']
    ctx.project_allocation_start_date = ctx.content['projectStartDate']
    ctx.project_centre = ctx.content['projectCentre']
    ctx.project_machine = ctx.content['projectMachine']
    ctx.project_name = ctx.content['projectName']
    ctx.project_node_type = ctx.content['projectNodeType']
    ctx.simulation_uid = ctx.content['simuid']



def _persist_metric(ctx):
    """Persists metric info to db.

    """
    # Persist project info.
    ctx.project = dao.persist_project(
        ctx.project_allocation_end_date,
        ctx.project_allocation,
        ctx.project_allocation_start_date,
        ctx.project_centre,
        ctx.project_machine,
        ctx.project_name,
        ctx.project_node_type,
        )

    # Persist occupation store info.
    dao.persist_occupation_store(
        ctx.occupation_store_date,
        ctx.occupation_store_login,
        ctx.occupation_store_name,
        ctx.occupation_store_size
        )

    # Persist consumption by login info.
    dao.persist_consumption_by_login(
        ctx.project.id,
        ctx.consumption_by_login_date,
        ctx.consumption_by_login_login,
        ctx.consumption_by_login_total
        )

    # Persist consumption by project info.
    dao.persist_consumption_by_project(
        ctx.project.id,
        ctx.consumption_by_project_date,
        ctx.consumption_by_project_total
        )
