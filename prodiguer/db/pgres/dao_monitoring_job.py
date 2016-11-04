# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.dao_monitoring_jobs.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring job related data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import cast
from sqlalchemy import func
from sqlalchemy import Integer

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
        as_datetime_string(j.execution_end_date),       #0
        as_datetime_string(j.execution_start_date),     #1
        j.execution_state,                              #2
        j.id,                                           #3
        cast(j.is_compute_end, Integer),                #4
        cast(j.is_error, Integer),                      #5
        cast(j.is_im, Integer),                         #6
        j.typeof,                                       #7
        s.id                                            #8
        )
    qry = qry.join(s, j.simulation_uid == s.uid)
    qry = qry.filter(j.execution_start_date != None)
    qry = _apply_active_simulation_filter(qry, start_date)
    qry = qry.order_by(j.execution_start_date)

    return qry.all()


@decorators.validate(validator.validate_retrieve_active_job_periods)
def retrieve_active_job_periods(start_date=None):
    """Retrieves active job period update details from db.

    :param datetime.datetime start_date: Job execution start date.

    :returns: Job details.
    :rtype: list

    """
    jp = types.JobPeriod
    s = types.Simulation

    qry = session.raw_query(
        s.id,
        func.max(jp.period_date_begin)
        )
    qry = qry.join(jp, s.uid == jp.simulation_uid)
    qry = _apply_active_simulation_filter(qry, start_date)
    qry = qry.group_by(s.id)

    return qry.all()


@decorators.validate(validator.validate_retrieve_active_job_periods)
def retrieve_job_periods(uid):
    """Retrieves active job period update details from db.

    :param str uid: UID of simulation.

    :returns: Job details.
    :rtype: list

    """
    jp = types.JobPeriod

    qry = session.raw_query(
        jp.job_uid,
        jp.period_date_end
        )
    qry = qry.filter(jp.simulation_uid == unicode(uid))
    qry = qry.order_by(jp.period_date_end)

    return qry.all()


@decorators.validate(validator.validate_retrieve_job)
def retrieve_job(uid):
    """Retrieves job details from db.

    :param str uid: UID of job.

    :returns: Job details.
    :rtype: types.monitoring.Job

    """
    j = types.Job

    qry = session.query(j)
    qry = qry.filter(j.job_uid == unicode(uid))

    return qry.first()


def retrieve_jobs_by_interval(interval_start, interval_end):
    """Retrieves collection of jobs filtered by start datae interval.

    :param datetime interval_start: Interval start date.
    :param datetime interval_end: Interval end date.

    :returns: Job details.
    :rtype: list

    """
    j = types.Job

    qry = session.query(j)
    qry = qry.filter(j.execution_start_date >= interval_start)
    qry = qry.filter(j.execution_start_date < interval_end)

    return qry.all()


@decorators.validate(validator.validate_retrieve_latest_job_periods)
def retrieve_latest_job_period(uid):
    """Retrieves details of a simulation's most recent job period update.

    :param str uid: UID of simulation.

    :returns: Job period details.
    :rtype: list

    """
    jp = types.JobPeriod

    qry = session.raw_query(jp)
    qry = qry.filter(jp.simulation_uid == unicode(uid))
    qry = qry.order_by(jp.period_date_begin.desc())

    return qry.first()


@decorators.validate(validator.validate_retrieve_latest_job_period_counter)
def retrieve_latest_job_period_counter(uid):
    """Retrieves identifier of a simulation's most recent job period update.

    :param str uid: UID of simulation.

    :returns: Job period id, corresponding counter.
    :rtype: list

    """
    jp = types.JobPeriod

    qry = session.raw_query(jp.period_id)
    qry = qry.filter(jp.simulation_uid == unicode(uid))
    qry = qry.order_by(jp.period_date_begin.desc())
    rows = qry.all()

    return (rows[0][0], rows.count(rows[0])) if rows else (0, 0)



def _get_job_raw_query():
    """Returns a raw query over job table.

    """
    j = types.Job
    s = types.Simulation

    qry = session.raw_query(
        # ... core fields
        as_datetime_string(j.execution_end_date),       #0
        as_datetime_string(j.execution_start_date),     #1
        j.execution_state,                              #2
        j.id,                                           #3
        cast(j.is_compute_end, Integer),                #4
        cast(j.is_error, Integer),                      #5
        cast(j.is_im, Integer),                         #6
        j.typeof,                                       #7
        s.id,                                           #8
        # ... non-core fields
        j.accounting_project,                           #9
        j.job_uid,                                      #10
        j.post_processing_component,                    #11
        as_date_string(j.post_processing_date),         #12
        j.post_processing_dimension,                    #13
        j.post_processing_file,                         #14
        j.post_processing_name,                         #15
        j.scheduler_id,                                 #16
        j.submission_path,                              #17
        j.warning_delay,                                #18
        s.uid                                           #19
        )
    qry = qry.join(s, j.simulation_uid == s.uid)

    return qry


@decorators.validate(validator.validate_retrieve_job_subset)
def retrieve_job_subset(uid):
    """Retrieves a subset of job details from db.

    :param str uid: UID of job.

    :returns: A subset of job details.
    :rtype: tuple

    """
    j = types.Job

    qry = _get_job_raw_query()
    qry = qry.filter(j.job_uid == unicode(uid))

    return qry.first()


@decorators.validate(validator.validate_retrieve_simulation_jobs)
def retrieve_simulation_jobs(uid):
    """Retrieves job details from db.

    :param str uid: UID of simulation.

    :returns: List of jobs associated with a simulation.
    :rtype: list

    """
    j = types.Job

    qry = _get_job_raw_query()
    qry = qry.filter(j.simulation_uid == unicode(uid))
    qry = qry.filter(j.execution_start_date != None)
    qry = qry.order_by(j.execution_start_date)

    return qry.all()


@decorators.validate(validator.validate_persist_job_start)
def persist_job_start(
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
        instance.execution_state = instance.get_execution_state()

        # ... optional fields
        if accounting_project:
            instance.accounting_project = unicode(accounting_project)
        if post_processing_name:
            instance.post_processing_name = unicode(post_processing_name)
            instance.is_im = (post_processing_name == 'monitoring')
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


@decorators.validate(validator.validate_persist_job_end)
def persist_job_end(
    execution_end_date,
    is_compute_end,
    is_error,
    job_uid,
    simulation_uid
    ):
    """Persists job end information to db.

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
        instance.execution_state = instance.get_execution_state()

    return dao.persist(_assign, types.Job, lambda: retrieve_job(job_uid))


@decorators.validate(validator.validate_persist_job_period)
def persist_job_period(
    simulation_uid,
    job_uid,
    period_id,
    period_date_begin,
    period_date_end
    ):
    """Persists job period information to db.

    :param str simulation_uid: Simulation UID.
    :param str job_uid: Job UID.
    :param int period_id: Job period ordinal identifier.
    :param int period_date_begin: Date upon which job period began.
    :param int period_date_end: Date upon which job period ended.

    :returns: A new job period instance.
    :rtype: types.JobPeriod

    """
    instance = types.JobPeriod()
    instance.simulation_uid = unicode(simulation_uid)
    instance.job_uid = unicode(job_uid)
    instance.period_date_begin = period_date_begin
    instance.period_date_end = period_date_end
    instance.period_id = period_id

    return session.insert(instance)


def get_earliest_job():
    """Retrieves earliest job in database.

    """
    j = types.Job

    qry = session.query(j)
    qry = qry.filter(j.execution_start_date != None)
    qry = qry.order_by(j.execution_start_date)

    return qry.first()


def _apply_active_simulation_filter(qry, start_date):
    """Applies a filter limiting result set to active simulations.

    """
    s = types.Simulation

    qry = qry.filter(s.execution_start_date != None)
    qry = qry.filter(s.is_obsolete == False)
    if start_date:
        qry = qry.filter(s.execution_start_date >= start_date)

    return qry
