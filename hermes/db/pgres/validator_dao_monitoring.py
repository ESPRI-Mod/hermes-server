# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.validator_dao_monitoring.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring related data access operations validation.


.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from hermes import cv
from hermes.utils.validation import validate_bool
from hermes.utils.validation import validate_date
from hermes.utils.validation import validate_int
from hermes.utils.validation import validate_iterable
from hermes.utils.validation import validate_str
from hermes.utils.validation import validate_uid
from hermes.utils.validation import validate_ucode



def validate_delete_simulation(uid):
    """Function input validator: delete_simulation.

    """
    validate_uid(uid, "Simulation uid")


def validate_exists(uid):
    """Function input validator: exists.

    """
    validate_uid(uid, "Simulation uid")


def validate_persist_job_start(
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
    """Function input validator: persist_job_start.

    """
    if accounting_project is not None:
        validate_ucode(accounting_project, "Job accounting project")
    validate_int(warning_delay, "Job warning delay")
    validate_date(execution_start_date, 'Job execution start date')
    cv.validator.validate_job_type(job_type)
    validate_uid(job_uid, "Job uid")
    validate_uid(simulation_uid, "Simulation uid")
    if post_processing_name is not None:
        pass
    if post_processing_date is not None:
        validate_date(post_processing_date, "post_processing_date", "YYYYMMDD")
    if post_processing_dimension is not None:
        pass
    if post_processing_component is not None:
        pass
    if post_processing_file is not None:
        pass
    if scheduler_id is not None:
        pass
    if submission_path is not None:
        pass


def validate_persist_job_end(
    execution_end_date,
    is_compute_end,
    is_error,
    job_uid,
    simulation_uid,
    ):
    """Function input validator: persist_job_end.

    """
    validate_date(execution_end_date, "Job execution end date")
    validate_bool(is_compute_end, 'Job is compute end flag')
    validate_bool(is_error, 'Job is Error flag')
    validate_uid(job_uid, "Job uid")
    validate_uid(simulation_uid, "Simulation uid")


def validate_persist_late_job(
    job_uid,
    simulation_uid,
    ):
    """Function input validator: persist_late_job.

    """
    validate_uid(job_uid, "Job uid")
    validate_uid(simulation_uid, "Simulation uid")


def validate_persist_job_period(
    simulation_uid,
    job_uid,
    period_id,
    period_date_begin,
    period_date_end
    ):
    """Function input validator: persist_job_period.

    """
    validate_uid(simulation_uid, "Simulation uid")
    validate_uid(job_uid, "Job uid")
    validate_int(period_id, "Job period identifier")
    validate_int(period_date_begin, "Job period date begin")
    validate_int(period_date_end, "Job period date end")


def validate_persist_environment_metric(
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
    """Function input validator: persist_environment_metric.

    """

    def _validate_action_name():
        pass

    def _validate_action_timestamp():
        pass

    def _validate_dir(dir_):
        pass

    def _validate_duration_ms():
        pass

    def _validate_size_mb():
        pass

    def _validate_throughput_mb_s():
        pass

    validate_uid(job_uid, "Job uid")
    validate_uid(simulation_uid, "Simulation uid")

    _validate_action_name()
    _validate_action_timestamp()
    _validate_dir(dir_from)
    _validate_dir(dir_to)
    _validate_duration_ms()
    _validate_size_mb()
    _validate_throughput_mb_s()


def validate_persist_simulation_start(
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
    """Function input validator: persist_simulation_start.

    """
    cv.validator.validate_compute_node(compute_node)
    cv.validator.validate_compute_node_login(compute_node_login)
    cv.validator.validate_compute_node_machine(compute_node_machine)
    cv.validator.validate_experiment(experiment)
    cv.validator.validate_model(model)
    cv.validator.validate_simulation_space(space)
    if accounting_project is not None:
        validate_ucode(accounting_project, "Simulation accounting project")
    validate_date(execution_start_date, 'Simulation execution start date')

    if compute_node_raw is not None:
        validate_ucode(compute_node_raw, 'Simulation compute node (raw)')
    if compute_node_login_raw is not None:
        validate_ucode(compute_node_login_raw, 'Simulation compute node login (raw)')
    if compute_node_machine_raw is not None:
        validate_ucode(compute_node_machine_raw, 'Simulation compute node machine (raw)')
    if experiment_raw is not None:
        validate_ucode(experiment_raw, 'Simulation experiment (raw)')
    if model_raw is not None:
        validate_ucode(model_raw, 'Simulation model (raw)')
    if space_raw is not None:
        validate_ucode(space_raw, 'Simulation space (raw)')

    validate_ucode(name, "Simulation name")
    if output_end_date is not None:
        validate_date(output_end_date, 'Output end date')
    if output_start_date is not None:
        validate_date(output_start_date, 'Output start date')
    validate_uid(uid, "Simulation uid")

    if submission_path:
        validate_str(submission_path, "Submit directory and job localisation")
    if archive_path:
        validate_str(submission_path, "Output tree located on ARCHIVE")
    if storage_path:
        validate_str(submission_path, "Output tree located on STORAGE")
    if storage_small_path:
        validate_str(submission_path, "Output tree located on STORAGE hosting figures")


def validate_persist_simulation_end(execution_end_date, is_error, uid):
    """Function input validator: persist_simulation_end.

    """
    validate_bool(is_error, 'Simulation Is Error flag')
    if execution_end_date is not None:
        validate_date(execution_end_date, "Simulation execution end date")
    validate_uid(uid, "Simulation uid")


def validate_persist_simulation_configuration(uid, card):
    """Function input validator: persist_simulation_configuration.

    """
    validate_uid(uid, "Simulation uid")
    validate_str(card, "Simulation config card")


def validate_retrieve_active_simulation(hashid):
    """Function input validator: retrieve_active_simulation.

    """
    validate_ucode(hashid, "Simulation hash identifier")


def validate_retrieve_active_simulations(start_date=None):
    """Function input validator: retrieve_active_simulations.

    """
    if start_date is not None:
        validate_date(start_date, 'Simulation execution start date')


def validate_retrieve_latest_active_job_periods(start_date=None, simulation_identifers=None):
    """Function input validator: retrieve_latest_active_job_periods.

    """
    if start_date is not None:
        validate_date(start_date, 'Execution start date')
    if simulation_identifers is not None:
        validate_iterable(simulation_identifers, "Simulation identifers")
        for simulation_identifer in simulation_identifers:
            validate_int(simulation_identifer, "Simulation id")


def validate_retrieve_job(uid):
    """Function input validator: retrieve_job.

    """
    validate_uid(uid, "Job uid")


def validate_retrieve_job_info(uid):
    """Function input validator: retrieve_job_info.

    """
    validate_uid(uid, "Job uid")

def validate_retrieve_latest_job_periods(uid):
    """Function input validator: retrieve_latest_job_periods.

    """
    validate_uid(uid, "Simulation uid")


def validate_retrieve_latest_job_period_counter(uid):
    """Function input validator: retrieve_latest_job_period_counter.

    """
    validate_uid(uid, "Simulation uid")


def validate_retrieve_simulation(uid):
    """Function input validator: retrieve_simulation.

    """
    validate_uid(uid, "Simulation uid")


def validate_retrieve_simulation_configuration(uid):
    """Function input validator: retrieve_simulation_configuration.

    """
    validate_uid(uid, "Simulation uid")


def validate_retrieve_simulation_jobs(uid):
    """Function input validator: retrieve_simulation_jobs.

    """
    validate_uid(uid, "Simulation uid")


def validate_retrieve_simulation_try(hashid, try_id):
    """Function input validator: retrieve_simulation_try.

    """
    validate_ucode(hashid, "Simulation hash identifier")
    validate_int(try_id, "Simulation try id")


def validate_retrieve_simulation_previous_tries(hashid, try_id):
    """Function input validator: retrieve_simulation_previous_tries.

    """
    validate_ucode(hashid, "Simulation hash identifier")
    validate_int(try_id, "Simulation try id")


def validate_update_active_simulation(hashid):
    """Function input validator: update_active_simulation.

    """
    validate_ucode(hashid, "Simulation hash identifier")
