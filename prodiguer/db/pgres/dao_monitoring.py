# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import distinct

from prodiguer.db.pgres import dao
from prodiguer.db.pgres import session
from prodiguer.db.pgres import types
from prodiguer.db.pgres import validator_dao_monitoring as validator
from prodiguer.db.pgres.convertor import as_date_string
from prodiguer.db.pgres.convertor import as_datetime_string
from prodiguer.utils import decorators



@decorators.validate(validator.validate_retrieve_active_jobs)
def retrieve_active_jobs(start_date=None):
    """Retrieves active job details from db.

    :param datetime.datetime start_date: Job execution start date.

    :returns: Job details.
    :rtype: list

    """
    j = types.Job
    s = types.Simulation

    qry = session.raw_query(
        as_datetime_string(j.execution_end_date),
        as_datetime_string(j.execution_start_date),
        j.is_compute_end,
        j.is_error,
        j.job_uid,
        j.simulation_uid,
        j.typeof,
        j.post_processing_name
        )
    qry = qry.join(s, j.simulation_uid == s.uid)
    qry = qry.filter(j.execution_start_date != None)
    qry = qry.filter(s.execution_start_date != None)
    qry = qry.filter(s.is_obsolete == False)
    if start_date:
        qry = qry.filter(s.execution_start_date >= start_date)
    qry = qry.order_by(j.execution_start_date)

    return qry.all()


@decorators.validate(validator.validate_retrieve_active_simulation)
def retrieve_active_simulation(hashid):
    """Retrieves an active simulation from db.

    :param str hashid: Simulation hash identifier.

    :returns: An active simulation instance.
    :rtype: types.Simulation

    """
    qry = session.query(types.Simulation)
    qry = qry.filter(types.Simulation.hashid == hashid)
    qry = qry.filter(types.Simulation.is_obsolete == False)

    return dao.exec_query(types.Simulation, qry)


@decorators.validate(validator.validate_retrieve_active_simulations)
def retrieve_active_simulations(start_date=None):
    """Retrieves active simulation details from db.

    :param datetime.datetime start_date: Simulation execution start date.

    :returns: Simulation details.
    :rtype: list

    """
    s = types.Simulation

    qry = session.raw_query(
        s.activity,
        s.activity_raw,
        s.compute_node_login,
        s.compute_node_login_raw,
        s.compute_node_machine,
        s.compute_node_machine_raw,
        as_datetime_string(s.execution_end_date),
        as_datetime_string(s.execution_start_date),
        s.experiment,
        s.experiment_raw,
        s.is_error,
        s.hashid,
        s.model,
        s.model_raw,
        s.name,
        as_date_string(s.output_end_date),
        as_date_string(s.output_start_date),
        s.space,
        s.space_raw,
        s.try_id,
        s.uid
        )
    qry = qry.filter(s.execution_start_date != None)
    qry = qry.filter(s.is_obsolete == False)
    if start_date:
        qry = qry.filter(s.execution_start_date >= start_date)
    qry = qry.order_by(s.execution_start_date)

    return qry.all()


@decorators.validate(validator.validate_retrieve_simulation)
def retrieve_simulation(uid):
    """Retrieves simulation details from db.

    :param str uid: UID of simulation.

    :returns: Simulation details.
    :rtype: types.monitoring.Simulation

    """
    qfilter = types.Simulation.uid == unicode(uid)

    return dao.get_by_facet(types.Simulation, qfilter=qfilter)


@decorators.validate(validator.validate_retrieve_simulation_try)
def retrieve_simulation_try(hashid, try_id):
    """Retrieves simulation details from db.

    :param str hashid: Simulation hash identifier.
    :param int try_id: Simulation try identifier.

    :returns: Simulation details.
    :rtype: types.monitoring.Simulation

    """
    qry = session.query(types.Simulation)
    qry = qry.filter(types.Simulation.hashid == unicode(hashid))
    qry = qry.filter(types.Simulation.try_id == int(try_id))

    return dao.exec_query(types.Simulation, qry)


@decorators.validate(validator.validate_exists)
def exists(uid):
    """Returns a flag indicating whether simulation already exists.

    :param str uid: UID of simulation.

    :returns: True if simulation exists false otherwise.
    :rtype: bool

    """
    qfilter = types.Simulation.uid == unicode(uid)

    return dao.get_count(types.Simulation, qfilter=qfilter) == 1


@decorators.validate(validator.validate_retrieve_simulation_configuration)
def retrieve_simulation_configuration(uid):
    """Retrieves simulation configuration details from db.

    :param str uid: UID of simulation.

    :returns: Simulation configuration details.
    :rtype: types.monitoring.SimulationConfiguration

    """
    qfilter = types.SimulationConfiguration.simulation_uid == unicode(uid)

    return dao.get_by_facet(types.SimulationConfiguration, qfilter=qfilter)


@decorators.validate(validator.validate_retrieve_simulation_jobs)
def retrieve_simulation_jobs(uid):
    """Retrieves job details from db.

    :param str uid: UID of simulation.

    :returns: List of jobs associated with a simulation.
    :rtype: list

    """
    j = types.Job

    qry = session.raw_query(
        as_datetime_string(j.execution_end_date),
        as_datetime_string(j.execution_start_date),
        j.is_compute_end,
        j.is_error,
        j.job_uid,
        j.simulation_uid,
        j.typeof,
        j.accounting_project,
        j.post_processing_component,
        as_date_string(j.post_processing_date),
        j.post_processing_dimension,
        j.post_processing_file,
        j.post_processing_name,
        j.scheduler_id,
        j.submission_path,
        j.warning_delay
        )
    qry = qry.filter(j.execution_start_date != None)
    qry = qry.filter(j.simulation_uid == unicode(uid))
    qry = qry.order_by(j.execution_start_date)

    return qry.all()


@decorators.validate(validator.validate_retrieve_job)
def retrieve_job(uid):
    """Retrieves job details from db.

    :param str uid: UID of job.

    :returns: Job details.
    :rtype: types.monitoring.Job

    """
    qry = session.query(types.Job)
    qry = qry.filter(types.Job.job_uid == unicode(uid))

    return qry.first()


def retrieve_jobs_by_interval(interval_start, interval_end):
    """Retrieves collection of jobs filtered by start datae interval.

    :param datetime interval_start: Interval start date.
    :param datetime interval_end: Interval end date.

    :returns: Job details.
    :rtype: list

    """
    qry = session.query(types.Job)
    qry = qry.filter(types.Job.execution_start_date >= interval_start)
    qry = qry.filter(types.Job.execution_start_date < interval_end)

    return qry.all()


@decorators.validate(validator.validate_retrieve_job_subset)
def retrieve_job_subset(uid):
    """Retrieves a subset of job details from db.

    :param str uid: UID of job.

    :returns: A subset of job details.
    :rtype: tuple

    """
    j = types.Job

    qry = session.raw_query(
        as_datetime_string(j.execution_end_date),
        as_datetime_string(j.execution_start_date),
        j.is_compute_end,
        j.is_error,
        j.job_uid,
        j.simulation_uid,
        j.typeof,
        j.accounting_project,
        j.post_processing_component,
        as_date_string(j.post_processing_date),
        j.post_processing_dimension,
        j.post_processing_file,
        j.post_processing_name,
        j.scheduler_id,
        j.submission_path,
        j.warning_delay
        )
    qry = qry.filter(j.job_uid == unicode(uid))

    return qry.one()


@decorators.validate(validator.validate_persist_environment_metric)
def persist_environment_metric(
    action_name,
    action_timestamp,
    dir_from,
    dir_to,
    duration_ms,
    job_uid,
    simulation_uid,
    size_mb,
    throughput_mb_s
    ):
    """Persists environment metric information to db.

    :param str action_name: Name of libIGCM action.
    :param str dir_from: Directory from which data was copied.
    :param str dir_to: Directory to which data was copied.
    :param integer duration_ms: Duration in milliseconds of action.
    :param str job_uid: Job UID.
    :param str simulation_uid: Simulation UID.
    :param float size_mb: Size in megabytes of moved file(s).
    :param datetime timestamp: Time when action took place.
    :param float throughput_mb_s: Rate at which copy took place.

    :returns: A new environment metrics instance.
    :rtype: types.EnvironmentMetric

    """
    instance = types.EnvironmentMetric()
    instance.action_name = unicode(action_name)
    instance.action_timestamp = action_timestamp
    instance.dir_from = unicode(dir_from)
    instance.dir_to = unicode(dir_to)
    instance.duration_ms = duration_ms
    instance.job_uid = unicode(job_uid)
    instance.simulation_uid = unicode(simulation_uid)
    instance.size_mb = size_mb
    instance.throughput_mb_s = throughput_mb_s

    return session.add(instance)


@decorators.validate(validator.validate_persist_simulation_01)
def persist_simulation_01(
    accounting_project,
    activity,
    activity_raw,
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
    uid
    ):
    """Persists simulation information to db.

    :param str accounting_project: Name of associated accounting project.
    :param str activity: Name of activity, e.g. IPSL.
    :param str activity_raw: Name of activity before CV reformatting.
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

    :returns: Either a new or an updated simulation instance.
    :rtype: types.Simulation

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.accounting_project = unicode(accounting_project)
        instance.activity = unicode(activity)
        instance.activity_raw = unicode(activity_raw)
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

    return dao.persist(_assign, types.Simulation, lambda: retrieve_simulation(uid))


@decorators.validate(validator.validate_persist_simulation_02)
def persist_simulation_02(execution_end_date, is_error, uid):
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


@decorators.validate(validator.validate_persist_simulation_configuration)
def persist_simulation_configuration(uid, card):
    """Persists a new simulation configuration db record.

    :param str uid: Simulation UID.
    :param str card: Simulation configuration card.

    """
    instance = types.SimulationConfiguration()
    instance.simulation_uid = unicode(uid)
    instance.card = unicode(card)

    return session.add(instance)


@decorators.validate(validator.validate_persist_command)
def persist_command(
    simulation_uid,
    job_uid,
    command_uid,
    timestamp,
    instruction,
    is_error):
    """Persists a new command db record.

    :param str simulation_uid: Simulation UID.
    :param str job_uid: Job UID.
    :param str uid: Command UID.
    :param datetime.datetime timestamp: Moment when command was executed.
    :param str instruction: Command instruction (including parameters).
    :param bool is_error: Flag indicating whether command was in error or not.

    """
    # Instantiate instance.
    instance = types.Command()
    instance.simulation_uid = unicode(simulation_uid)
    instance.job_uid = unicode(job_uid)
    instance.uid = unicode(command_uid)
    instance.timestamp = timestamp
    instance.instruction = instruction
    instance.is_error = is_error

    return session.add(instance)


@decorators.validate(validator.validate_persist_job_01)
def persist_job_01(
    accounting_project,
    warning_delay,
    execution_start_date,
    job_type,
    job_uid,
    simulation_uid,
    post_processing_name=None,
    post_processing_date=None,
    post_processing_dimension=None,
    post_processing_component=None,
    post_processing_file=None,
    scheduler_id=None,
    submission_path=None
    ):
    """Persists job information to db.

    :param str accounting_project: Name of associated accounting project.
    :param int warning_delay: Delay before a job is considered to be late.
    :param datetime execution_start_date: Simulation start date.
    :param str job_type: Job type.
    :param str job_uid: Job UID.
    :param str simulation_uid: Simulation UID.
    :param str post_processing_name: Post processing job name.
    :param str post_processing_date: Post processing job name.
    :param str post_processing_dimension: Post processing job name.
    :param str post_processing_component: Post processing job name.
    :param str post_processing_file: Post processing job name.
    :param str scheduler_id: ID attributed by the scheduler to this job.
    :param str submission_path:  Submit directory and job localisation.

    :returns: Either a new or an updated job instance.
    :rtype: types.Job

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.execution_start_date = execution_start_date
        instance.typeof = unicode(job_type)
        instance.job_uid = unicode(job_uid)
        instance.simulation_uid = unicode(simulation_uid)
        instance.warning_delay = int(warning_delay)

        # ... optional fields
        if accounting_project:
            instance.accounting_project = unicode(accounting_project)
        if post_processing_name:
            instance.post_processing_name = unicode(post_processing_name)
        if post_processing_date:
            instance.post_processing_date = post_processing_date
        if post_processing_dimension:
            instance.post_processing_dimension = unicode(post_processing_dimension)
        if post_processing_component:
            instance.post_processing_component = unicode(post_processing_component)
        if post_processing_file:
            instance.post_processing_file = unicode(post_processing_file)
        if scheduler_id:
            instance.scheduler_id = unicode(scheduler_id)
        if submission_path:
            instance.submission_path = unicode(submission_path)

    return dao.persist(_assign, types.Job, lambda: retrieve_job(job_uid))


@decorators.validate(validator.validate_persist_job_02)
def persist_job_02(
    execution_end_date,
    is_compute_end,
    is_error,
    job_uid,
    simulation_uid):
    """Persists job information to db.

    :param datetime execution_end_date: Job end date.
    :param bool is_compute_end: Flag indicating whether a simulation has ended or not.
    :param bool is_error: Flag indicating whether the job terminated in error.
    :param str job_uid: Job unique identifier.
    :param str simulation_uid: Simulation UID.

    :returns: Either a new or an updated job instance.
    :rtype: types.Job

    """
    def _assign(instance):
        """Assigns instance values from input parameters.

        """
        instance.execution_end_date = execution_end_date
        instance.is_compute_end = is_compute_end
        instance.is_error = is_error
        instance.job_uid = unicode(job_uid)
        instance.simulation_uid = unicode(simulation_uid)

    return dao.persist(_assign, types.Job, lambda: retrieve_job(job_uid))


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
    dao.delete_by_facet(types.Job, types.Job.simulation_uid == uid)
    dao.delete_by_facet(types.SimulationConfiguration, types.SimulationConfiguration.simulation_uid == uid)
    dao.delete_by_facet(types.Message, types.Message.correlation_id_1 == uid)
    dao.delete_by_facet(types.Simulation, types.Simulation.uid == uid)


def get_accounting_projects():
    """Retrieves disinct set of accounting projects.

    """
    j = types.Job

    qry = session.raw_query(
        distinct(j.accounting_project)
        )

    return set(sorted([ap[0] for ap in qry.all()]))


def get_earliest_job():
    """Retrieves earliest job in database.

    """
    j = types.Job

    qry = session.query(types.Job)
    qry = qry.filter(types.Job.execution_start_date is not None)
    qry = qry.order_by(types.Job.execution_start_date)

    return qry.first()
