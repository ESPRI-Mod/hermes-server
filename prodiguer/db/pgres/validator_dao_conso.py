# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring_conso.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Conso related data access operations validator.


.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import cv
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
    pass


def validate_persist_consumption(
    allocation_id,
    date,
    total_hrs,
    login=None
    ):
    """Function input validator: persist_consumption_by_login.

    """
    pass


def validate_persist_occupation_store(
    date,
    login,
    name,
    size
    ):
    """Function input validator: persist_occupation_store.

    """
    pass


def validate_retrieve_allocation(
    centre,
    machine,
    node_type,
    project,
    start_date
    ):
    """Function input validator: retrieve_allocation.

    """
    pass
