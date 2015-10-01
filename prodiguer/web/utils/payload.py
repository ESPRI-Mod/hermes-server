# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.utils.payload.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Payload utility functions shared by endpoints.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import cv
from prodiguer.db import pgres as db
from prodiguer.utils import config



def _delete_fields(data, fields):
    """Deletes fields from data.

    """
    for field in fields:
        del data[field]


def _delete_null_fields(data, fields):
    """Deletes null fields from data.

    """
    _delete_fields(data, [f for f in fields if data[f] is None])


def _delete_false_fields(data, fields):
    """Deletes false fields from data.

    """
    _delete_fields(data, [f for f in fields if data[f] == False])


def trim_simulation(data):
    """Trims size of a simulation being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.Simulation data: Simulation being returned to front-end.

    :returns: Trimmed simulation.
    :rtype: dict

    """
    # Convert to trimmed dictionary.
    data = db.utils.get_item(data)

    # Delete fields not required by front end
    _delete_null_fields(data, {
        'output_start_date',
        'output_end_date',
        'ensemble_member'
        })

    # Delete null fields.
    _delete_null_fields(data, {
        'accounting_project',
        'execution_end_date',
        'parent_simulation_branch_date',
        'parent_simulation_name',
        'experiment',
        'model',
        'space'
        })

    # Delete null cv fields
    _delete_null_fields(data, {
        'activity',
        'activity_raw',
        'compute_node',
        'compute_node_raw',
        'compute_node_login',
        'compute_node_login_raw',
        'compute_node_machine',
        'compute_node_machine_raw',
        'experiment',
        'experiment_raw',
        'model',
        'model_raw',
        'space',
        'space_raw'
        })

    # Delete false fields
    _delete_false_fields(data, {
        'is_error',
        'is_obsolete'
        })

    return data


def trim_job(data):
    """Trims size of a job being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.Job data: Job being returned to front-end.

    :returns: Trimmed job.
    :rtype: dict

    """
    # Convert to trimmed dictionary.
    data = db.utils.get_item(data)

    # Delete null fields
    _delete_null_fields(data, {
        'accounting_project',
        'execution_end_date',
        'post_processing_component',
        'post_processing_date',
        'post_processing_dimension',
        'post_processing_file',
        'post_processing_name'
        })

    # Delete false fields
    _delete_false_fields(data, {
        'is_error'
        })

    # Delete start up field for post-processing jobs
    if data['typeof'] != cv.constants.JOB_TYPE_COMPUTING:
        del data['is_startup']

    # Delete fields with matching defaults
    if data['typeof'] == cv.constants.JOB_TYPE_POST_PROCESSING:
        del data['typeof']
    if data['warning_delay'] == config.apps.monitoring.defaultJobWarningDelayInSeconds:
        del data['warning_delay']

    return data


def trim_message(data):
    """Trims size of a message being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.Message data: Message being returned to front-end.

    :returns: Trimmed message.
    :rtype: dict

    """
    # Convert to trimmed dictionary.
    data = db.utils.get_item(data)

    # Rename a field.
    data['job_uid'] = data['correlation_id_2']

    # Delete fields
    _delete_fields(data, {
        'app_id',
        'content_type',
        'content_encoding',
        'correlation_id_1',
        'correlation_id_2',
        'correlation_id_3',
        'producer_id',
        'timestamp_raw',
        'timestamp_precision',
        'user_id'
        })

    return data


def trim_term(data):
    """Trims size of a term being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.ControlledVocabularyTerm data: Term being returned to front-end.

    :returns: Trimmed message.
    :rtype: dict

    """
    # Convert to trimmed dictionary.
    data = db.utils.get_item(data)

    # Delete null fields
    _delete_null_fields(data, {
        'sort_key',
        'synonyms'
        })

    return data
