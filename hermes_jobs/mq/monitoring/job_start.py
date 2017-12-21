# -*- coding: utf-8 -*-

"""
.. module:: monitoring_job_start.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes messages emitted by libIGCM whenever a job starts.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

import arrow
import pytz
from sqlalchemy.exc import IntegrityError

from hermes import cv
from hermes import mq
from hermes.cv.constants import JOB_TYPE_COMPUTING
from hermes.cv.constants import JOB_TYPE_POST_PROCESSING
from hermes.db import pgres as db
from hermes.db.pgres import dao_monitoring as dao
from hermes.db.pgres.constants import DEFAULT_TZ
from hermes.utils import config
from hermes.utils import logger
from hermes_jobs.mq import utils as mq_utils



# Set of CV related simulation fields.
_SIMULATION_CV_TERM_FIELDS = {
    'accounting_project',
    'compute_node',
    'compute_node_login',
    'compute_node_machine',
    'experiment',
    'model',
    'simulation_space'
}

# Set of lower case CV related simulation fields.
_SIMULATION_CV_TERM_FIELDS_LOWER_CASE = {
    'compute_node',
    'compute_node_machine',
    'model',
    'simulation_space'
}

# Map of message to job types.
_MESSAGE_JOB_TYPES = {
    mq.constants.MESSAGE_TYPE_0000: JOB_TYPE_COMPUTING,
    mq.constants.MESSAGE_TYPE_1000: JOB_TYPE_COMPUTING,
    mq.constants.MESSAGE_TYPE_2000: JOB_TYPE_POST_PROCESSING
}


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack,
        _parse_cv,
        _persist_cv,
        _persist_simulation,
        _update_simulation_im_flag,
        _enqueue_cv_git_push,
        _enqueue_fe_notification,
        _enqueue_late_job_detection
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True, validate_props=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(props, body, decode=decode, validate_props=validate_props)

        self.cv_terms = []
        self.cv_terms_for_fe = []
        self.cv_terms_new = []
        self.cv_terms_persisted_to_db = []

        self.accounting_project = None
        self.compute_node = self.compute_node_raw = None
        self.compute_node_login = self.compute_node_login_raw = None
        self.compute_node_machine = self.compute_node_machine_raw = None
        self.experiment = self.experiment_raw = None
        self.model = self.model_raw = None
        self.simulation_space = self.simulation_space_raw = None

        self.active_simulation = None
        self.is_simulation_start = props.type == mq.constants.MESSAGE_TYPE_0000
        self.is_compute_job_start = props.type == mq.constants.MESSAGE_TYPE_1000
        self.job = None
        self.job_type = _MESSAGE_JOB_TYPES[self.props.type]
        self.job_uid = None
        self.job_warning_delay = None
        self.post_processing_date = None
        self.simulation_uid = None


def _unpack(ctx):
    """Unpacks message content.

    """
    ctx.accounting_project = ctx.get_field('accountingProject')
    ctx.job_uid = ctx.content['jobuid']
    ctx.job_warning_delay = ctx.get_field(
        'jobWarningDelay', config.apps.monitoring.defaultJobWarningDelayInSeconds)
    if ctx.job_warning_delay in ("0", ""):
        ctx.job_warning_delay = config.apps.monitoring.defaultJobWarningDelayInSeconds
    ctx.simulation_uid = ctx.content['simuid']
    if ctx.is_simulation_start:
        ctx.compute_node = ctx.compute_node_raw = ctx.content['centre']
        ctx.compute_node_login = ctx.content['login']
        ctx.compute_node_machine = ctx.compute_node_machine_raw = \
            "{0}-{1}".format(ctx.compute_node, ctx.content['machine'])
        ctx.experiment = ctx.experiment_raw = ctx.content['experiment'] or "--"
        ctx.model = ctx.model_raw = ctx.content['model']
        ctx.simulation_space = ctx.simulation_space_raw = ctx.content['space']
        for field in _SIMULATION_CV_TERM_FIELDS_LOWER_CASE:
            setattr(ctx, field, getattr(ctx, field).lower())
        if ctx.simulation_space_raw == "":
            ctx.simulation_space_raw = None
    ctx.post_processing_date = ctx.get_field('postProcessingDate')
    if ctx.post_processing_date and ctx.post_processing_date.endswith('0230'):
        logger.log_mq('post processing date: february date subsitution')
        ctx.post_processing_date = ctx.post_processing_date.replace('0230', '0228')


def _parse_cv(ctx):
    """Parses cv terms contained within message content.

    """
    # Skip if unnecessary.
    if not ctx.is_simulation_start:
        return

    for term_type in _SIMULATION_CV_TERM_FIELDS:
        term_name = getattr(ctx, term_type)
        try:
            cv.validator.validate_term_name(term_type, term_name)
        except cv.TermNameError:
            ctx.cv_terms_new.append(cv.create(term_type, term_name))
        else:
            parsed_term_name = cv.parser.parse_term_name(term_type, term_name)
            if term_name != parsed_term_name:
                setattr(ctx, term_type, parsed_term_name)
                msg = "CV term subsitution: {0}.{1} --> {0}.{2}"
                msg = msg.format(term_type, term_name, parsed_term_name)
                logger.log_mq(msg)
                term_name = parsed_term_name
            ctx.cv_terms.append(cv.cache.get_term(term_type, term_name))


def _persist_cv(ctx):
    """Parses cv terms contained within message content.

    """
    # Escape if unnecessary.
    if not ctx.is_simulation_start:
        return

    # Commit cv session.
    cv.session.insert(ctx.cv_terms_new)
    cv.session.commit()

    # Reparse.
    ctx.cv_terms = []
    _parse_cv(ctx)

    # Persist to database.
    for term in ctx.cv_terms:
        persisted_term = None
        try:
            persisted_term = db.dao_cv.create_term(
                term['meta']['type'],
                term['meta']['name'],
                term['meta'].get('display_name', None),
                term['meta']['uid']
                )
        except IntegrityError:
            db.session.rollback()
        finally:
            if persisted_term:
                ctx.cv_terms_persisted_to_db.append(persisted_term)

    # Sets cv terms to be passed to front-end.
    for term in ctx.cv_terms_persisted_to_db:
        ctx.cv_terms_for_fe.append({
            'typeof': term.typeof,
            'name': term.name,
            'displayName': term.display_name,
            'synonyms': term.synonyms,
            'uid': term.uid,
            'sortKey': term.sort_key
            })


def _persist_simulation(ctx):
    """Persists job/simulation information to db.

    """
    def _get_output_date(field):
        """Parses an output date.

        """
        raw_date = ctx.content[field]
        try:
            return arrow.get(raw_date).datetime.replace(tzinfo=None)
        except (arrow.parser.ParserError, ValueError) as err:
            logger.log_mq_warning('{} date is unparseable [{}]'.format(field, raw_date))
            return None

    # Persist job info.
    ctx.job = dao.persist_job_start(
        ctx.accounting_project,
        ctx.job_warning_delay,
        ctx.msg.timestamp,      # execution_start_date
        ctx.job_type,
        ctx.job_uid,
        ctx.simulation_uid,
        post_processing_component=ctx.get_field('postProcessingComp'),
        post_processing_date=ctx.post_processing_date,
        post_processing_dimension=ctx.get_field('postProcessingDimn'),
        post_processing_file=ctx.get_field('postProcessingFile'),
        post_processing_name=ctx.get_field('postProcessingName'),
        scheduler_id=ctx.get_field('jobSchedulerID'),
        submission_path=ctx.get_field('jobSubmissionPath')
        )

    # Persist simulation info.
    if ctx.is_simulation_start:
        # ... simulation.
        simulation = dao.persist_simulation_start(
            ctx.accounting_project,
            ctx.compute_node,
            ctx.compute_node_raw,
            ctx.compute_node_login,
            ctx.compute_node_login_raw,
            ctx.compute_node_machine,
            ctx.compute_node_machine_raw,
            ctx.msg.timestamp,  # execution_start_date
            ctx.experiment,
            ctx.experiment_raw,
            ctx.model,
            ctx.model_raw,
            ctx.content['name'],
            _get_output_date('startDate'),
            _get_output_date('endDate'),
            ctx.simulation_space,
            ctx.simulation_space_raw,
            ctx.simulation_uid,
            ctx.get_field('jobSubmissionPath'),
            ctx.get_field('archivePath'),
            ctx.get_field('storagePath'),
            ctx.get_field('storageSmallPath')
            )

        # ... configuration.
        config_card = ctx.get_field('configuration')
        if config_card:
            dao.persist_simulation_configuration(
                ctx.simulation_uid,
                config_card
                )

        # ... active simulation.
        ctx.active_simulation = \
            dao.update_active_simulation(simulation.hashid)

    # Commit to database.
    db.session.commit()


def _update_simulation_im_flag(ctx):
    """Updates simulation inter-monitoring flag info to db.

    """
    if ctx.job.is_im:
        dao.update_simulation_im_flag(ctx.simulation_uid, True)
        db.session.commit()


def _enqueue_cv_git_push(ctx):
    """Places a message upon the new cv terms notification queue.

    """
    # Escape if unnecessary.
    if not ctx.is_simulation_start:
        return
    if not ctx.cv_terms_persisted_to_db and not ctx.cv_terms_new:
        return

    mq_utils.enqueue(mq.constants.MESSAGE_TYPE_CV)


def _enqueue_fe_notification(ctx):
    """Places a message upon the front-end notification queue.

    """
    if ctx.is_simulation_start:
        event_type = 'simulation_start'
        simulation_uid = ctx.active_simulation.uid
    else:
        event_type = 'job_start'
        simulation_uid = ctx.simulation_uid

    mq_utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": event_type,
        "cv_terms": ctx.cv_terms_for_fe,
        "job_uid": ctx.job_uid,
        "simulation_uid": simulation_uid
    })


def _enqueue_late_job_detection(ctx):
    """Places a delayed message upon the supervisor detection queue.

    """
    # TODO: Set max historical latency offset in
    #       order to account for buffered 1100 messages.
    if ctx.is_compute_job_start:
        pass

    # Calculate time delta until system must check if job is late or not.
    delta_in_s = int((ctx.job.warning_limit - datetime.datetime.now()).total_seconds())

    print ctx.job.execution_start_date, ctx.job.warning_delay, ctx.job.warning_limit, datetime.datetime.now(), delta_in_s

    if delta_in_s < 0:
        delta_in_s = 600    # 10 minutes for historical messages
    else:
        delta_in_s += 300   # +5 mins for potential job end latency

    # Log.
    msg = "Enqueuing late job detection: sim-uid = {}; job-uid = {}; delay = {}s"
    msg = msg.format(ctx.simulation_uid, ctx.job_uid, delta_in_s)
    logger.log_mq(msg)

    # Enqueue.
    mq_utils.enqueue(
        mq.constants.MESSAGE_TYPE_8000,
        delay_in_ms=delta_in_s * 1000,
        payload={
            "job_uid": ctx.job_uid,
            "simulation_uid": ctx.simulation_uid,
            "trigger_code": ctx.props.type
        }
    )