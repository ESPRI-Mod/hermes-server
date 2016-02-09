# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_conso.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: CONSO data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import arrow

from prodiguer.db.pgres import dao
from prodiguer.db.pgres import session
from prodiguer.db.pgres import types
from prodiguer.db.pgres import validator_dao_conso as validator
from prodiguer.utils import decorators



@decorators.validate(validator.validate_persist_allocation)
def persist_allocation(
    centre,
    end_date,
    machine,
    node_type,
    project,
    start_date,
    total_hrs,
    ):
    """Persists allocation information to db.

    :param str centre: Name of associated accounting project.
    :param datetime end_date: Name of activity, e.g. IPSL.
    :param str machine: Name of activity before CV reformatting.
    :param str node_type: Name of compute node, e.g. TGCC.
    :param str project: Name of compute node before CV reformatting.
    :param datetime start_date: Name of compute node login, e.g. dcugnet.
    :param float total_hrs: Name of compute node login before CV reformatting.

    :returns: Either a new or an updated allocation instance.
    :rtype: types.Allocation

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.centre = unicode(centre)
        instance.end_date = arrow.get(end_date).datetime
        instance.machine = unicode(machine)
        instance.node_type = unicode(node_type)
        instance.project = unicode(project)
        instance.start_date = arrow.get(start_date).datetime
        instance.total_hrs = float(total_hrs)

    def _retrieve():
        """Returns instance from input parameters.

        """
        return retrieve_allocation(
            centre,
            machine,
            node_type,
            project,
            start_date
            )

    return dao.persist(_assign, types.Allocation, _retrieve)


@decorators.validate(validator.validate_persist_consumption)
def persist_consumption(
    allocation_id,
    date,
    total_hrs,
    login=None
    ):
    instance = types.Consumption()
    instance.allocation_id = allocation_id
    instance.date = arrow.get(date).datetime
    instance.total_hrs = float(total_hrs)
    if login is not None:
        instance.login = unicode(login)

    return session.insert(instance)


@decorators.validate(validator.validate_persist_occupation_store)
def persist_occupation_store(
    date,
    login,
    name,
    size_gb
    ):
    instance = types.OccupationStore()
    instance.date = arrow.get(date).datetime
    instance.login = unicode(login)
    instance.name = unicode(name)
    instance.size_gb = float(size_gb)

    return session.insert(instance)


@decorators.validate(validator.validate_retrieve_allocation)
def retrieve_allocation(
    centre,
    machine,
    node_type,
    project,
    start_date
    ):
    """Retrieves allocation information from db.

    :param str centre: Name of associated accounting project.
    :param str machine: Name of activity before CV reformatting.
    :param str node_type: Name of compute node, e.g. TGCC.
    :param str project: Name of compute node before CV reformatting.
    :param datetime start_date: Name of compute node login, e.g. dcugnet.

    :returns: An allocation instance if found else None.
    :rtype: types.Allocation | None

    """
    qry = session.query(types.Allocation)
    qry = qry.filter(types.Allocation.centre == unicode(centre))
    qry = qry.filter(types.Allocation.machine == unicode(machine))
    qry = qry.filter(types.Allocation.node_type == unicode(node_type))
    qry = qry.filter(types.Allocation.project == unicode(project))
    qry = qry.filter(types.Allocation.start_date == arrow.get(start_date).datetime)

    return qry.first()



def retrieve_jobs_by_project():
    raise NotImplemented()



def retrieve_mails_by_project():
    raise NotImplemented()
