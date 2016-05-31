# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.validator_dao_monitoring.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring related data access operations validation.


.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import cv
from prodiguer.utils import validation



def validate_delete_simulation(uid):
    """Function input validator: delete_simulation.

    """
    validation.validate_uid(uid, "Simulation uid")


def validate_exists(uid):
    """Function input validator: exists.

    """
    validation.validate_uid(uid, "Simulation uid")


def validate_persist_command(
    simulation_uid,
    job_uid,
    command_uid,
    timestamp,
    instruction,
    is_error):
    """Function input validator: persist_command.

    """
    validation.validate_bool(is_error, 'Is Error flag')
    validation.validate_unicode(instruction, 'Command instruction')
    validation.validate_date(timestamp, 'Command timestamp')
    validation.validate_uid(command_uid, "Command uid")
    validation.validate_uid(job_uid, "Job uid")
    validation.validate_uid(simulation_uid, "Simulation uid")


def validate_persist_job_01(
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
    """Function input validator: persist_job_01.

    """
    if accounting_project is not None:
        validation.validate_unicode(accounting_project, "Job accounting project")
    validation.validate_int(warning_delay, "Job warning delay")
    validation.validate_date(execution_start_date, 'Job execution start date')
    cv.validator.validate_job_type(job_type)
    validation.validate_uid(job_uid, "Job uid")
    validation.validate_uid(simulation_uid, "Simulation uid")
    if post_processing_name is not None:
        pass
    if post_processing_date is not None:
        validation.validate_date(post_processing_date, "post_processing_date", "YYYYMMDD")
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


def validate_persist_job_02(
    execution_end_date,
    is_compute_end,
    is_error,
    job_uid,
    simulation_uid,
    ):
    """Function input validator: persist_job_02.

    """
    validation.validate_date(execution_end_date, "Job execution end date")
    validation.validate_bool(is_compute_end, 'Job is compute end flag')
    validation.validate_bool(is_error, 'Job is Error flag')
    validation.validate_uid(job_uid, "Job uid")
    validation.validate_uid(simulation_uid, "Simulation uid")


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

    validation.validate_uid(job_uid, "Job uid")
    validation.validate_uid(simulation_uid, "Simulation uid")

    _validate_action_name()
    _validate_action_timestamp()
    _validate_dir(dir_from)
    _validate_dir(dir_to)
    _validate_duration_ms()
    _validate_size_mb()
    _validate_throughput_mb_s()


def validate_persist_simulation_01(
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
    """Function input validator: persist_simulation_01.

    """
    cv.validator.validate_activity(activity)
    cv.validator.validate_compute_node(compute_node)
    cv.validator.validate_compute_node_login(compute_node_login)
    cv.validator.validate_compute_node_machine(compute_node_machine)
    cv.validator.validate_experiment(experiment)
    cv.validator.validate_model(model)
    cv.validator.validate_simulation_space(space)
    if accounting_project is not None:
        validation.validate_unicode(accounting_project, "Simulation accounting project")
    validation.validate_date(execution_start_date, 'Simulation execution start date')

    if activity_raw is not None:
        validation.validate_unicode(activity_raw, 'Simulation activity (raw)')
    if compute_node_raw is not None:
        validation.validate_unicode(compute_node_raw, 'Simulation compute node (raw)')
    if compute_node_login_raw is not None:
        validation.validate_unicode(compute_node_login_raw, 'Simulation compute node login (raw)')
    if compute_node_machine_raw is not None:
        validation.validate_unicode(compute_node_machine_raw, 'Simulation compute node machine (raw)')
    if experiment_raw is not None:
        validation.validate_unicode(experiment_raw, 'Simulation experiment (raw)')
    if model_raw is not None:
        validation.validate_unicode(model_raw, 'Simulation model (raw)')
    if space_raw is not None:
        validation.validate_unicode(space_raw, 'Simulation space (raw)')

    validation.validate_unicode(name, "Simulation name")
    validation.validate_date(output_end_date, 'Output end date')
    validation.validate_date(output_start_date, 'Output start date')
    validation.validate_uid(uid, "Simulation uid")


def validate_persist_simulation_02(execution_end_date, is_error, uid):
    """Function input validator: persist_simulation_02.

    """
    validation.validate_bool(is_error, 'Simulation Is Error flag')
    if execution_end_date is not None:
        validation.validate_date(execution_end_date, "Simulation execution end date")
    validation.validate_uid(uid, "Simulation uid")


def validate_persist_simulation_configuration(uid, card):
    """Function input validator: persist_simulation_configuration.

    """
    validation.validate_uid(uid, "Simulation uid")
    validation.validate_str(card, "Simulation config card")


def validate_retrieve_active_simulation(hashid):
    """Function input validator: retrieve_active_simulation.

    """
    validation.validate_unicode(hashid, "Simulation hash identifier")


def validate_retrieve_active_simulations(start_date=None):
    """Function input validator: retrieve_active_simulations.

    """
    if start_date is not None:
        validation.validate_date(start_date, 'Simulation execution start date')


def validate_retrieve_active_jobs(start_date=None):
    """Function input validator: update_active_jobs.

    """
    if start_date is not None:
        validation.validate_date(start_date, 'Job execution start date')


def validate_retrieve_job(uid):
    """Function input validator: retrieve_job.

    """
    validation.validate_uid(uid, "Job uid")


def validate_retrieve_job_subset(uid):
    """Function input validator: retrieve_job_subset.

    """
    validation.validate_uid(uid, "Job uid")


def validate_retrieve_simulation(uid):
    """Function input validator: retrieve_simulation.

    """
    validation.validate_uid(uid, "Simulation uid")


def validate_retrieve_simulation_configuration(uid):
    """Function input validator: retrieve_simulation_configuration.

    """
    validation.validate_uid(uid, "Simulation uid")


def validate_retrieve_simulation_jobs(uid):
    """Function input validator: retrieve_simulation_jobs.

    """
    validation.validate_uid(uid, "Simulation uid")


def validate_retrieve_simulation_try(hashid, try_id):
    """Function input validator: retrieve_simulation_try.

    """
    validation.validate_unicode(hashid, "Simulation hash identifier")
    validation.validate_int(try_id, "Simulation try id")


def validate_retrieve_simulation_previous_tries(hashid, try_id):
    """Function input validator: retrieve_simulation_previous_tries.

    """
    validation.validate_unicode(hashid, "Simulation hash identifier")
    validation.validate_int(try_id, "Simulation try id")


def validate_update_active_simulation(hashid):
    """Function input validator: update_active_simulation.

    """
    validation.validate_unicode(hashid, "Simulation hash identifier")
