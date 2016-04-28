# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring_conso.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Conso related data access operations validation.


.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.utils import validation as v



def validate_persist_allocation(
    centre,
    project,
    sub_project,
    machine,
    node_type,
    start_date,
    end_date,
    total_hrs,
    is_active,
    is_reviewed
    ):
    """Function input validator: persist_project.

    """
    v.validate_unicode(centre, 'Allocation computing centre')
    v.validate_unicode(machine, 'Allocation machine')
    v.validate_unicode(node_type, 'Allocation node type')
    v.validate_unicode(project, 'Allocation project')
    if sub_project:
        v.validate_unicode(sub_project, 'Allocation sub-project')
    v.validate_date(start_date, 'Allocation start date')
    v.validate_date(end_date, 'Allocation end date')
    v.validate_float(total_hrs, 'Allocation total (hours)')
    v.validate_bool(is_active, 'Allocation is-active flag')
    v.validate_bool(is_reviewed, 'Allocation is-reviewed flag')


def validate_retrieve_allocation(
    centre,
    project,
    machine,
    node_type,
    consumption_date
    ):
    """Function input validator: retrieve_allocation.

    """
    v.validate_unicode(centre, 'Allocation computing centre')
    v.validate_unicode(project, 'Allocation project')
    v.validate_unicode(machine, 'Allocation machine')
    v.validate_unicode(node_type, 'Allocation node type')
    v.validate_date(consumption_date, 'Resource consumption date')


def validate_persist_consumption(
    allocation_id,
    date,
    total_hrs,
    login=None,
    batch_date=None
    ):
    """Function input validator: persist_consumption_by_login.

    """
    v.validate_int(allocation_id, 'Allocation identifier')
    v.validate_date(date, 'Consumption date')
    v.validate_float(total_hrs, 'Consumption total (hours)')
    if login is not None:
        v.validate_unicode(login, 'Consumption login')
    if batch_date is not None:
        v.validate_date(batch_date, 'Consumption batch date')


def validate_persist_occupation_store(
    date,
    login,
    name,
    size_gb
    ):
    """Function input validator: persist_occupation_store.

    """
    v.validate_date(date, 'Occupation store date')
    v.validate_unicode(login, 'Occupation store login')
    v.validate_unicode(name, 'Occupation name')
    v.validate_float(size_gb, 'Occupation size (GB)')
