# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring_conso.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Conso related data access operations validation.


.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.utils import validation



def validate_persist_allocation(
    centre,
    end_date,
    machine,
    node_type,
    project,
    start_date,
    total_hrs
    ):
    """Function input validator: persist_project.

    """
    validation.validate_unicode(centre, 'Allocation computing centre')
    validation.validate_date(end_date, 'Allocation end date')
    validation.validate_unicode(machine, 'Allocation machine')
    validation.validate_unicode(node_type, 'Allocation node type')
    validation.validate_unicode(project, 'Allocation project')
    validation.validate_date(start_date, 'Allocation start date')
    validation.validate_float(total_hrs, 'Allocation total (hours)')


def validate_persist_consumption(
    allocation_id,
    date,
    total_hrs,
    login=None
    ):
    """Function input validator: persist_consumption_by_login.

    """
    validation.validate_int(allocation_id, 'Allocation identifier')
    validation.validate_date(date, 'Consumption date')
    validation.validate_float(total_hrs, 'Consumption total (hours)')
    if login is not None:
        validation.validate_unicode(login, 'Consumption login')


def validate_persist_occupation_store(
    date,
    login,
    name,
    size_gb
    ):
    """Function input validator: persist_occupation_store.

    """
    validation.validate_date(date, 'Occupation store date')
    validation.validate_unicode(login, 'Occupation store login')
    validation.validate_unicode(name, 'Occupation name')
    validation.validate_float(size_gb, 'Occupation size (GB)')


def validate_retrieve_allocation(
    centre,
    machine,
    node_type,
    project,
    start_date
    ):
    """Function input validator: retrieve_allocation.

    """
    validation.validate_unicode(centre, 'Allocation computing centre')
    validation.validate_unicode(machine, 'Allocation machine')
    validation.validate_unicode(node_type, 'Allocation node type')
    validation.validate_unicode(project, 'Allocation project')
    validation.validate_date(start_date, 'Allocation start date')
