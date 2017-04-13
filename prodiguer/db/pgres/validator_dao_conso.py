# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.dao_monitoring_conso.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Conso related data access operations validation.


.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from hermes.utils.validation import validate_bool
from hermes.utils.validation import validate_date
from hermes.utils.validation import validate_float
from hermes.utils.validation import validate_int
from hermes.utils.validation import validate_ucode



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
    validate_ucode(centre, 'Allocation computing centre')
    validate_ucode(machine, 'Allocation machine')
    validate_ucode(node_type, 'Allocation node type')
    validate_ucode(project, 'Allocation project')
    if sub_project:
        validate_ucode(sub_project, 'Allocation sub-project')
    validate_date(start_date, 'Allocation start date')
    validate_date(end_date, 'Allocation end date')
    validate_float(total_hrs, 'Allocation total (hours)')
    validate_bool(is_active, 'Allocation is-active flag')
    validate_bool(is_reviewed, 'Allocation is-reviewed flag')


def validate_retrieve_allocation(
    centre,
    project,
    machine,
    node_type,
    consumption_date
    ):
    """Function input validator: retrieve_allocation.

    """
    validate_ucode(centre, 'Allocation computing centre')
    validate_ucode(project, 'Allocation project')
    validate_ucode(machine, 'Allocation machine')
    validate_ucode(node_type, 'Allocation node type')
    validate_date(consumption_date, 'Resource consumption date')


def validate_persist_consumption(
    allocation_id,
    date,
    total_hrs,
    login=None,
    sub_project=None,
    batch_date=None
    ):
    """Function input validator: persist_consumption.

    """
    validate_int(allocation_id, 'Allocation identifier')
    validate_date(date, 'Consumption date')
    validate_float(total_hrs, 'Consumption total (hours)')
    if login is not None:
        validate_ucode(login, 'Consumption login')
    if sub_project is not None:
        validate_ucode(sub_project, 'Consumption sub project')
    if batch_date is not None:
        validate_date(batch_date, 'Consumption batch date')


def validate_retrieve_consumption_header(
    allocation_id,
    date
    ):
    """Function input validator: retrieve_consumption_header.

    """
    validate_int(allocation_id, 'Allocation identifier')
    validate_date(date, 'Consumption date')
