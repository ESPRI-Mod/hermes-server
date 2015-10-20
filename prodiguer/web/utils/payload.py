# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.utils.payload.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Payload utility functions shared by endpoints.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

from prodiguer import cv
from prodiguer.db import pgres as db
from prodiguer.utils import config



def _delete_fields(obj, fields):
    """Deletes fields from data.

    """
    for field in fields:
        del obj[field]

    return obj


def _delete_null_fields(obj, fields):
    """Deletes null fields from data.

    """
    _delete_fields(obj, [f for f in fields if obj[f] is None])


def _delete_false_fields(obj, fields):
    """Deletes false fields from data.

    """
    _delete_fields(obj, [f for f in fields if obj[f] == False])


def _convert_to_dict(instance):
    """Returns an instance of a mapped db row as a dictionary.

    """
    return _delete_fields(db.convertor.convert(instance), {
        'id',
        'row_create_date',
        'row_update_date'
        })


def trim_simulation(instance):
    """Trims size of a simulation being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.Simulation instance: Simulation being returned to front-end.

    :returns: Trimmed simulation.
    :rtype: dict

    """
    # Convert to dictionary.
    obj = _convert_to_dict(instance)

    # Delete fields not required by front end
    _delete_null_fields(obj, {
        'output_start_date',
        'output_end_date',
        'ensemble_member'
        })

    # Delete null fields.
    _delete_null_fields(obj, {
        'accounting_project',
        'execution_end_date',
        'parent_simulation_branch_date',
        'parent_simulation_name',
        'experiment',
        'model',
        'space'
        })

    # Delete null cv fields
    _delete_null_fields(obj, {
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
    _delete_false_fields(obj, {
        'is_error',
        'is_obsolete'
        })

    return obj


def trim_job(instance, full_trim=False):
    """Trims size of a job being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.Job instance: Job being returned to front-end.
    :param bool full_trim: Flag indicating whether the job is to be fully trimmed or not.

    :returns: Trimmed job.
    :rtype: dict

    """
    # Convert to dictionary.
    obj = _convert_to_dict(instance)

    _delete_null_fields(obj, {'execution_end_date'})
    _delete_false_fields(obj, {'is_error'})
    if obj['typeof'] == cv.constants.JOB_TYPE_POST_PROCESSING:
        del obj['typeof']

    if full_trim == True:
        _delete_fields(obj, {
            'accounting_project',
            'is_compute_start',
            'post_processing_component',
            'post_processing_date',
            'post_processing_dimension',
            'post_processing_file',
            'post_processing_name',
            'scheduler_id',
            'submission_path',
            'warning_delay'
            })
    else:
        _delete_null_fields(obj, {
            'accounting_project',
            'post_processing_component',
            'post_processing_date',
            'post_processing_dimension',
            'post_processing_file',
            'post_processing_name',
            'scheduler_id',
            'submission_path'
            })
        if obj['warning_delay'] == config.apps.monitoring.defaultJobWarningDelayInSeconds:
            del obj['warning_delay']
        if obj.get('typeof') != cv.constants.JOB_TYPE_COMPUTING:
            del obj['is_compute_start']
            del obj['is_compute_end']

    return obj


def trim_message(instance):
    """Trims size of a message being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.Message instance: Message being returned to front-end.

    :returns: Trimmed message.
    :rtype: dict

    """
    # Convert to dictionary.
    obj = _convert_to_dict(instance)

    # Inject fields.
    obj['job_uid'] = instance.correlation_id_2
    obj['processed'] = instance.row_create_date

    # Delete fields
    _delete_fields(obj, {
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

    return obj


def trim_term(instance):
    """Trims size of a term being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.ControlledVocabularyTerm instance: Term being returned to front-end.

    :returns: Trimmed message.
    :rtype: dict

    """
    # Convert to dictionary.
    obj = _convert_to_dict(instance)

    # Delete null fields
    _delete_null_fields(obj, {
        'sort_key',
        'synonyms'
        })

    return obj
