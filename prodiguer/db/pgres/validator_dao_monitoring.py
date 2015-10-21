# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_monitoring_validator.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Monitoring related data access operations validator.


.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import cv
from prodiguer.db.pgres import validator



def validate_delete_simulation(uid):
    """Function input validator: delete_simulation.

    """
    validator.validate_simulation_uid(uid)


def validate_exists(uid):
    """Function input validator: exists.

    """
    validator.validate_simulation_uid(uid)


def validate_persist_command(
    simulation_uid,
    job_uid,
    command_uid,
    timestamp,
    instruction,
    is_error):
    """Function input validator: persist_command.

    """
    validator.validate_bool(is_error, 'Is Error flag')
    validator.validate_unicode(instruction, 'Command instruction')
    validator.validate_date(timestamp, 'Command timestamp')
    validator.validate_uid(command_uid, "Command uid")
    validator.validate_job_uid(job_uid)
    validator.validate_simulation_uid(simulation_uid)


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
    validator.validate_accounting_project(accounting_project)
    validator.validate_job_warning_delay(warning_delay)
    validator.validate_execution_start_date(execution_start_date)
    cv.validator.validate_job_type(job_type)
    validator.validate_job_uid(job_uid)
    validator.validate_simulation_uid(simulation_uid)
    if post_processing_name is not None:
        pass
    if post_processing_date is not None:
        validator.validate_date(post_processing_date, "post_processing_date", "YYYYMMDD")
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
    validator.validate_execution_end_date(execution_end_date)
    validator.validate_bool(is_compute_end, 'Is compute end flag')
    validator.validate_bool(is_error, 'Is Error flag')
    validator.validate_job_uid(job_uid)
    validator.validate_simulation_uid(simulation_uid)


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

    validator.validate_job_uid(job_uid)
    validator.validate_simulation_uid(simulation_uid)

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
    validator.validate_accounting_project(accounting_project)
    validator.validate_execution_start_date(execution_start_date)
    validator.validate_raw_activity(activity_raw)
    validator.validate_raw_compute_node(compute_node_raw)
    validator.validate_raw_compute_node_login(compute_node_login_raw)
    validator.validate_raw_compute_node_machine(compute_node_machine_raw)
    validator.validate_raw_experiment(experiment_raw)
    validator.validate_raw_model(model_raw)
    validator.validate_raw_simulation_space(space_raw)
    validator.validate_simulation_name(name)
    validator.validate_simulation_output_start_date(output_start_date)
    validator.validate_simulation_output_end_date(output_end_date)
    validator.validate_simulation_uid(uid)


def validate_persist_simulation_02(execution_end_date, is_error, uid):
    """Function input validator: persist_simulation_02.

    """
    validator.validate_bool(is_error, 'Is Error flag')
    if execution_end_date is not None:
        validator.validate_execution_end_date(execution_end_date)
    validator.validate_simulation_uid(uid)


def validate_persist_simulation_configuration(uid, card):
    """Function input validator: persist_simulation_configuration.

    """
    validator.validate_simulation_uid(uid)
    validator.validate_simulation_configuration_card(card)


def validate_retrieve_active_simulation(hashid):
    """Function input validator: retrieve_active_simulation.

    """
    validator.validate_simulation_hashid(hashid)


def validate_retrieve_active_simulations(start_date=None):
    """Function input validator: retrieve_active_simulations.

    """
    if start_date is not None:
        validator.validate_execution_start_date(start_date)


def validate_retrieve_active_jobs(start_date=None):
    """Function input validator: update_active_jobs.

    """
    if start_date is not None:
        validator.validate_execution_start_date(start_date)


def validate_retrieve_job(uid):
    """Function input validator: retrieve_job.

    """
    validator.validate_job_uid(uid)


def validate_retrieve_simulation(uid):
    """Function input validator: retrieve_simulation.

    """
    validator.validate_simulation_uid(uid)


def validate_retrieve_simulation_configuration(uid):
    """Function input validator: retrieve_simulation_configuration.

    """
    validator.validate_simulation_uid(uid)


def validate_retrieve_simulation_messages(uid):
    """Function input validator: retrieve_simulation_messages.

    """
    validator.validate_simulation_uid(uid)


def validate_retrieve_simulation_message_count(uid):
    """Function input validator: retrieve_simulation_message_count.

    """
    validator.validate_simulation_uid(uid)


def validate_retrieve_simulation_jobs(uid):
    """Function input validator: retrieve_simulation_jobs.

    """
    validator.validate_simulation_uid(uid)


def validate_retrieve_simulation_try(hashid, try_id):
    """Function input validator: retrieve_simulation_try.

    """
    validator.validate_simulation_hashid(hashid)
    validator.validate_simulation_try_id(try_id)


def validate_update_active_simulation(hashid):
    """Function input validator: update_active_simulation.

    """
    validator.validate_simulation_hashid(hashid)
