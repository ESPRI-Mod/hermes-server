# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.dao_monitoring.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import cast
from sqlalchemy import Integer

from prodiguer.db.pgres import dao
from prodiguer.db.pgres import session
from prodiguer.db.pgres import types
from prodiguer.db.pgres import validator_dao_monitoring as validator
from prodiguer.db.pgres.convertor import as_date_string
from prodiguer.db.pgres.convertor import as_datetime_string
from prodiguer.utils import decorators



@decorators.validate(validator.validate_retrieve_active_simulation)
def retrieve_active_simulation(hashid):
    """Retrieves an active simulation from db.

    :param str hashid: Simulation hash identifier.

    :returns: An active simulation instance.
    :rtype: types.Simulation

    """
    s = types.Simulation

    qry = session.query(s)
    qry = qry.filter(s.hashid == hashid)
    qry = qry.filter(s.is_obsolete == False)

    return dao.exec_query(s, qry)


@decorators.validate(validator.validate_retrieve_active_simulations)
def retrieve_active_simulations(start_date=None):
    """Retrieves active simulation details from db.

    :param datetime.datetime start_date: Simulation execution start date.

    :returns: Simulation details.
    :rtype: list

    """
    s = types.Simulation
    qry = session.raw_query(
        s.accounting_project,                           #0
        s.compute_node_login,                           #1
        s.compute_node_machine,                         #2
        as_datetime_string(s.execution_end_date),       #3
        as_datetime_string(s.execution_start_date),     #4
        s.experiment,                                   #5
        s.experiment_raw,                               #6
        s.is_error,                                     #7
        s.hashid,                                       #8
        s.id,                                           #9
        s.model,                                        #10
        s.model_raw,                                    #11
        s.name,                                         #12
        s.space,                                        #13
        s.space_raw,                                    #14
        s.try_id,                                       #15
        s.uid,                                          #16
        as_date_string(s.output_start_date),            #17
        as_date_string(s.output_end_date),              #18
        cast(s.is_im, Integer),                         #19
        )
    qry = qry.filter(s.execution_start_date != None)
    qry = qry.filter(s.is_obsolete == False)
    if start_date:
        qry = qry.filter(s.execution_start_date >= start_date)
    qry = qry.order_by(s.execution_start_date.desc())

    return qry.all()


@decorators.validate(validator.validate_retrieve_simulation)
def retrieve_simulation(uid):
    """Retrieves simulation details from db.

    :param str uid: UID of simulation.

    :returns: Simulation details.
    :rtype: types.monitoring.Simulation

    """
    s = types.Simulation

    qfilter = s.uid == unicode(uid)

    return dao.get_by_facet(s, qfilter=qfilter)


@decorators.validate(validator.validate_retrieve_simulation_try)
def retrieve_simulation_try(hashid, try_id):
    """Retrieves simulation details from db.

    :param str hashid: Simulation hash identifier.
    :param int try_id: Simulation try identifier.

    :returns: Simulation details.
    :rtype: types.monitoring.Simulation

    """
    s = types.Simulation

    qry = session.query(s)
    qry = qry.filter(s.hashid == unicode(hashid))
    qry = qry.filter(s.try_id == int(try_id))

    return dao.exec_query(s, qry)


@decorators.validate(validator.validate_retrieve_simulation_previous_tries)
def retrieve_simulation_previous_tries(hashid, try_id):
    """Retrieves simulation details from db.

    :param str hashid: Simulation hash identifier.
    :param int try_id: Simulation try identifier.

    :returns: List of try identifiers and simulation uid's.
    :rtype: types.monitoring.Simulation

    """
    s = types.Simulation

    qry = session.raw_query(
        s.try_id,
        s.uid
        )
    qry = qry.filter(types.Simulation.hashid == unicode(hashid))
    qry = qry.filter(types.Simulation.try_id < int(try_id))

    return qry.all()


@decorators.validate(validator.validate_retrieve_simulation_configuration)
def retrieve_simulation_configuration(uid):
    """Retrieves simulation configuration details from db.

    :param str uid: UID of simulation.

    :returns: Simulation configuration details.
    :rtype: types.monitoring.SimulationConfiguration

    """
    qfilter = types.SimulationConfiguration.simulation_uid == unicode(uid)

    return dao.get_by_facet(types.SimulationConfiguration, qfilter=qfilter)


@decorators.validate(validator.validate_persist_simulation_start)
def persist_simulation_start(
    accounting_project,
    compute_node,
    compute_node_raw,
    compute_node_login,
    compute_node_login_raw,
    compute_node_machine,
    compute_node_machine_raw,
    execution_start_date,
    experiment,
    experiment_raw,
    model,
    model_raw,
    name,
    output_start_date,
    output_end_date,
    space,
    space_raw,
    uid,
    submission_path,
    archive_path,
    storage_path,
    storage_small_path
    ):
    """Persists simulation information to db.

    :param str accounting_project: Name of associated accounting project.
    :param str compute_node: Name of compute node, e.g. TGCC.
    :param str compute_node_raw: Name of compute node before CV reformatting.
    :param str compute_node_login: Name of compute node login, e.g. dcugnet.
    :param str compute_node_login_raw: Name of compute node login before CV reformatting.
    :param str compute_node_machine: Name of compute machine, e.g. SX9.
    :param str compute_node_machine_raw: Name of compute node machine before CV reformatting.
    :param datetime execution_start_date: Simulation start date.
    :param str experiment: Name of experiment, e.g. piControl.
    :param str experiment_raw: Name of experiment before CV reformatting.
    :param str model: Name of model, e.g. IPSLCM5A.
    :param str model_raw: Name of model before CV reformatting.
    :param str name: Name of simulation, e.g. v3.aqua4K.
    :param datetime output_start_date: Output start date.
    :param datetime output_end_date: Output start date.
    :param str space: Name of space, e.g. PROD.
    :param str space_raw: Name of space before CV reformatting.
    :param str uid: Simulation unique identifier.
    :param str submission_path: Submit directory and job localisation.
    :param str archive_path: Output tree located on ARCHIVE.
    :param str storage_path: Output tree located on STORAGE.
    :param str storage_small_path: Output tree located on STORAGE hosting figures.

    :returns: Either a new or an updated simulation instance.
    :rtype: types.Simulation

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.accounting_project = unicode(accounting_project)
        instance.compute_node = unicode(compute_node)
        instance.compute_node_raw = unicode(compute_node_raw)
        instance.compute_node_login = unicode(compute_node_login)
        instance.compute_node_login_raw = unicode(compute_node_login_raw)
        instance.compute_node_machine = unicode(compute_node_machine)
        instance.compute_node_machine_raw = unicode(compute_node_machine_raw)
        instance.execution_start_date = execution_start_date
        instance.experiment = unicode(experiment)
        instance.experiment_raw = unicode(experiment_raw)
        instance.model = unicode(model)
        instance.model_raw = unicode(model_raw)
        instance.name = unicode(name)
        instance.output_start_date = output_start_date
        instance.output_end_date = output_end_date
        instance.space = unicode(space)
        instance.space_raw = unicode(space_raw)
        instance.uid = unicode(uid)
        instance.hashid = instance.get_hashid()
        if submission_path:
            instance.submission_path = unicode(submission_path)
        if archive_path:
            instance.archive_path = unicode(archive_path)
        if storage_path:
            instance.storage_path = unicode(storage_path)
        if storage_small_path:
            instance.storage_small_path = unicode(storage_small_path)

    return dao.persist(_assign, types.Simulation, lambda: retrieve_simulation(uid))


@decorators.validate(validator.validate_persist_simulation_end)
def persist_simulation_end(execution_end_date, is_error, uid):
    """Persists simulation information to db.

    :param datetime execution_end_date: Simulation end date.
    :param bool is_error: Flag indicating whether the simulation terminated in error.
    :param str uid: Simulation unique identifier.

    :returns: Either a new or an updated simulation instance.
    :rtype: types.Simulation

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.execution_end_date = execution_end_date
        instance.is_error = is_error
        instance.uid = unicode(uid)

    return dao.persist(_assign, types.Simulation, lambda: retrieve_simulation(uid))


def update_simulation_im_flag(uid, is_im):
    """Updates a simulation's inter-monitoring flag.

    :param str uid: Simulation unique identifier.
    :param bool is_im: Flag indicating whether the simulation has inter-monitoring jobs.

    """
    instance = retrieve_simulation(uid)
    if instance:
        instance.is_im = is_im

    return instance


@decorators.validate(validator.validate_persist_simulation_configuration)
def persist_simulation_configuration(uid, card):
    """Persists a new simulation configuration db record.

    :param str uid: Simulation UID.
    :param str card: Simulation configuration card.

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.simulation_uid = unicode(uid)
        instance.card = unicode(card)

    return dao.persist(_assign, types.SimulationConfiguration, lambda: retrieve_simulation_configuration(uid))


@decorators.validate(validator.validate_update_active_simulation)
def update_active_simulation(hashid):
    """Updates the active simulation within a group.

    :param str hashid: A simulation hash identifier used to group a batch of simulations.

    """
    # Set simulation group.
    qry = session.query(types.Simulation)
    qry = qry.filter(types.Simulation.hashid == hashid)
    group = sorted(qry.all(), key=lambda s: s.execution_start_date)

    # Update try identifier & obsolete flag.
    for index, simulation in enumerate(group, 1):
        simulation.try_id = index
        simulation.is_obsolete = (index != len(group))

    # Return active.
    return group[-1]


@decorators.validate(validator.validate_delete_simulation)
def delete_simulation(uid):
    """Deletes a simulation from database.

    """
    dao.delete_by_facet(types.EnvironmentMetric, types.EnvironmentMetric.simulation_uid == uid)
    dao.delete_by_facet(types.Job, types.Job.simulation_uid == uid)
    dao.delete_by_facet(types.JobPeriod, types.JobPeriod.simulation_uid == uid)
    dao.delete_by_facet(types.SimulationConfiguration, types.SimulationConfiguration.simulation_uid == uid)
    dao.delete_by_facet(types.Message, types.Message.correlation_id_1 == uid)
    dao.delete_by_facet(types.Simulation, types.Simulation.uid == uid)
    dao.delete_by_facet(types.Supervision, types.Supervision.simulation_uid == uid)


def get_simulation_accounting_project(uid):
    """Retrieves disinct set of accounting projects for the passed simulation set.

    """
    s = types.Simulation

    qry = session.raw_query(
        s.accounting_project
        )
    qry = qry.filter(s.uid == uid)

    return qry.first()


def get_simulation_accounting_projects():
    """Retrieves map of simulation id's and accounting projects.

    """
    s = types.Simulation

    qry = session.raw_query(
        s.uid,
        s.accounting_project
        )

    return qry.all()


