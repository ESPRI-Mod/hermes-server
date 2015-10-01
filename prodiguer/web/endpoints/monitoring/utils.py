# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.monitoring.utils.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Utility functions shared by monitoring endpoints.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import cv
from prodiguer.db import pgres as db
from prodiguer.utils import config



def trim_simulation(simulation):
    """Trims size of a simulation being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.Simulation simulation: Simulation being returned to front-end.

    :returns: Trimmed simulation.
    :rtype: dict

    """
    # Convert to trimmed dictionary.
    simulation = db.utils.get_item(simulation)

    # Delete fields not required by front end
    del simulation['output_start_date']
    del simulation['output_end_date']
    del simulation['ensemble_member']

    # Delete null fields
    for field in {
        'accounting_project',
        'execution_end_date',
        'parent_simulation_branch_date',
        'parent_simulation_name',
        'experiment',
        'model',
        'space'
        }:
        if simulation[field] is None:
            del simulation[field]

    # Delete null cv fields
    for field in {
        'activity',
        'compute_node',
        'compute_node_login',
        'compute_node_machine',
        'experiment',
        'model',
        'space'
        }:
        if simulation[field] is None:
            del simulation[field]
        if simulation["{}_raw".format(field)] is None:
            del simulation["{}_raw".format(field)]

    # Delete false fields
    for field in {
        'is_error',
        'is_obsolete'
        }:
        if simulation[field] == False:
            del simulation[field]

    return simulation


def trim_job(job):
    """Trims size of a job being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.Job job: Job being returned to front-end.

    :returns: Trimmed job.
    :rtype: dict

    """
    # Convert to trimmed dictionary.
    job = db.utils.get_item(job)

    # Delete null fields
    if job['accounting_project'] is None:
        del job['accounting_project']
    if job['execution_end_date'] is None:
        del job['execution_end_date']
    if job['is_error'] == False:
        del job['is_error']
    if job['post_processing_component'] is None:
        del job['post_processing_component']
    if job['post_processing_date'] is None:
        del job['post_processing_date']
    if job['post_processing_dimension'] is None:
        del job['post_processing_dimension']
    if job['post_processing_file'] is None:
        del job['post_processing_file']
    if job['post_processing_name'] is None:
        del job['post_processing_name']

    # Delete start up field for post-processing jobs
    if job['typeof'] != cv.constants.JOB_TYPE_COMPUTING:
        del job['is_startup']

    # Delete fields with matching defaults
    if job['typeof'] == cv.constants.JOB_TYPE_POST_PROCESSING:
        del job['typeof']
    if job['warning_delay'] == config.apps.monitoring.defaultJobWarningDelayInSeconds:
        del job['warning_delay']

    return job


def trim_message(msg):
    """Trims size of a message being returned to front-end by removing superfluos information.

    :param prodiguer.db.pgres.types.Message msg: Message being returned to front-end.

    :returns: Trimmed message.
    :rtype: dict

    """
    # Convert to trimmed dictionary.
    msg = db.utils.get_item(msg)

    msg['job_uid'] = msg['correlation_id_2']
    del msg['app_id']
    del msg['content_type']
    del msg['content_encoding']
    del msg['correlation_id_1']
    del msg['correlation_id_2']
    del msg['correlation_id_3']
    del msg['producer_id']
    del msg['timestamp_raw']
    del msg['timestamp_precision']
    del msg['user_id']

    return msg

