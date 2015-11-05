# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring_conso.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Conso related data access operations validator.


.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres import validator



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
    validator.validate_unicode(centre, 'Allocation computing centre')
    validator.validate_date(end_date, 'Allocation end date')
    validator.validate_unicode(machine, 'Allocation machine')
    validator.validate_unicode(node_type, 'Allocation node type')
    validator.validate_unicode(project, 'Allocation project')
    validator.validate_date(start_date, 'Allocation start date')
    validator.validate_float(total_hrs, 'Allocation total (hours)')


def validate_persist_consumption(
    allocation_id,
    date,
    total_hrs,
    login=None
    ):
    """Function input validator: persist_consumption_by_login.

    """
    validator.validate_int(allocation_id, 'Allocation identifier')
    validator.validate_date(date, 'Consumption date')
    validator.validate_float(total_hrs, 'Consumption total (hours)')
    if login is not None:
        validator.validate_unicode(login, 'Consumption login')


def validate_persist_occupation_store(
    date,
    login,
    name,
    size_gb
    ):
    """Function input validator: persist_occupation_store.

    """
    validator.validate_date(date, 'Occupation store date')
    validator.validate_unicode(login, 'Occupation store login')
    validator.validate_unicode(name, 'Occupation name')
    validator.validate_float(size_gb, 'Occupation size (GB)')


def validate_retrieve_allocation(
    centre,
    machine,
    node_type,
    project,
    start_date
    ):
    """Function input validator: retrieve_allocation.

    """
    validator.validate_unicode(centre, 'Allocation computing centre')
    validator.validate_unicode(machine, 'Allocation machine')
    validator.validate_unicode(node_type, 'Allocation node type')
    validator.validate_unicode(project, 'Allocation project')
    validator.validate_date(start_date, 'Allocation start date')
