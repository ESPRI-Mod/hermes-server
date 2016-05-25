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
from sqlalchemy.exc import IntegrityError

from prodiguer import cv
from prodiguer import mq
from prodiguer.cv.constants import JOB_TYPE_COMPUTING
from prodiguer.cv.constants import JOB_TYPE_POST_PROCESSING
from prodiguer.cv.constants import JOB_TYPE_POST_PROCESSING_FROM_CHECKER
from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer.utils import config
from prodiguer.utils import logger
from hermes_jobs.mq import utils as mq_utils



# Set of CV related simulation fields.
_SIMULATION_CV_TERM_FIELDS = {
    'accounting_project',
    'activity',
    'compute_node',
    'compute_node_login',
    'compute_node_machine',
    'experiment',
    'model',
    'simulation_space'
}

# Set of lower case CV related simulation fields.
_SIMULATION_CV_TERM_FIELDS_LOWER_CASE = {
    'activity',
    'compute_node',
    'compute_node_machine',
    'model',
    'simulation_space'
}

# Map of message to job types.
_MESSAGE_JOB_TYPES = {
    mq.constants.MESSAGE_TYPE_0000: JOB_TYPE_COMPUTING,
    mq.constants.MESSAGE_TYPE_1000: JOB_TYPE_COMPUTING,
    mq.constants.MESSAGE_TYPE_2000: JOB_TYPE_POST_PROCESSING,
    mq.constants.MESSAGE_TYPE_3000: JOB_TYPE_POST_PROCESSING_FROM_CHECKER
}


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _parse_cv,
        _persist_cv,
        _persist_job,
        _persist_simulation,
        _enqueue_cv_git_push,
        _enqueue_late_job_detection,
        _enqueue_fe_notification
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.cv_terms = []
        self.cv_terms_for_fe = []
        self.cv_terms_new = []
        self.cv_terms_persisted_to_db = []

        self.accounting_project = None
        self.activity = self.activity_raw = None
        self.compute_node = self.compute_node_raw = None
        self.compute_node_login = self.compute_node_login_raw = None
        self.compute_node_machine = self.compute_node_machine_raw = None
        self.experiment = self.experiment_raw = None
        self.model = self.model_raw = None
        self.simulation_space = self.simulation_space_raw = None

        self.active_simulation = None
        self.is_simulation_start = props.type == mq.constants.MESSAGE_TYPE_0000
        self.job_type = _MESSAGE_JOB_TYPES[self.props.type]
        self.job_uid = None
        self.job_warning_delay = None
        self.simulation_uid = None


def _unpack_content(ctx):
    """Unpacks message content.

    """
    ctx.accounting_project = ctx.get_field('accountingProject')
    ctx.job_uid = ctx.content['jobuid']
    ctx.job_warning_delay = ctx.get_field(
        'jobWarningDelay', config.apps.monitoring.defaultJobWarningDelayInSeconds)
    if ctx.job_warning_delay == "0":
        ctx.job_warning_delay = config.apps.monitoring.defaultJobWarningDelayInSeconds
    ctx.simulation_uid = ctx.content['simuid']
    if ctx.is_simulation_start:
        ctx.activity = ctx.activity_raw = ctx.content['activity']
        ctx.compute_node = ctx.compute_node_raw = ctx.content['centre']
        ctx.compute_node_login = ctx.content['login']
        ctx.compute_node_machine = ctx.compute_node_machine_raw = \
            "{0}-{1}".format(ctx.compute_node, ctx.content['machine'])
        ctx.experiment = ctx.experiment_raw = ctx.content['experiment']
        ctx.model = ctx.model_raw = ctx.content['model']
        ctx.simulation_space = ctx.simulation_space_raw = ctx.content['space']
        for field in _SIMULATION_CV_TERM_FIELDS_LOWER_CASE:
            setattr(ctx, field, getattr(ctx, field).lower())


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
    # Skip if unnecessary.
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


def _persist_job(ctx):
    """Persists job info to db.

    """
    # Persist job.
    dao.persist_job_01(
        ctx.accounting_project,
        ctx.job_warning_delay,
        ctx.msg.timestamp,
        ctx.job_type,
        ctx.job_uid,
        ctx.simulation_uid,
        post_processing_component=ctx.get_field('postProcessingComp'),
        post_processing_date=ctx.get_field('postProcessingDate'),
        post_processing_dimension=ctx.get_field('postProcessingDimn'),
        post_processing_file=ctx.get_field('postProcessingFile'),
        post_processing_name=ctx.get_field('postProcessingName'),
        scheduler_id=ctx.get_field('jobSchedulerID'),
        submission_path=ctx.get_field('jobSubmissionPath')
        )

    # Update simulation (compute jobs only).
    if not ctx.is_simulation_start and ctx.job_type != JOB_TYPE_COMPUTING:
        dao.persist_simulation_02(
            None,
            False,
            ctx.simulation_uid
            )


def _persist_simulation(ctx):
    """Persists simulation information to db.

    """
    # Skip if unnecessary.
    if not ctx.is_simulation_start:
        return

    # Persist simulation.
    simulation = dao.persist_simulation_01(
        ctx.accounting_project,
        ctx.activity,
        ctx.activity_raw,
        ctx.compute_node,
        ctx.compute_node_raw,
        ctx.compute_node_login,
        ctx.compute_node_login_raw,
        ctx.compute_node_machine,
        ctx.compute_node_machine_raw,
        ctx.msg.timestamp,
        ctx.experiment,
        ctx.experiment_raw,
        ctx.model,
        ctx.model_raw,
        ctx.content['name'],
        arrow.get(ctx.content['startDate']).datetime,
        arrow.get(ctx.content['endDate']).datetime,
        ctx.simulation_space,
        ctx.simulation_space_raw,
        ctx.simulation_uid
        )

    # Persist simulation configuration.
    config_card = ctx.get_field('configuration')
    if config_card:
        dao.persist_simulation_configuration(
            ctx.simulation_uid,
            config_card
            )

    # Persist active simulation.
    ctx.active_simulation = \
        dao.update_active_simulation(simulation.hashid)
    db.session.commit()


def _enqueue_late_job_detection(ctx):
    """Places a delayed message upon the supervisor detection queue.

    """
    # Calculate expected job completion moment.
    expected = arrow.get(ctx.msg.timestamp) + \
               datetime.timedelta(seconds=int(ctx.job_warning_delay))

    # Calculate time delta until system must check if job is late or not.
    delta_in_s = int((expected - arrow.get()).total_seconds())
    if delta_in_s < 0:
        delta_in_s = 600    # 10 minutes for historical messages
    else:
        delta_in_s += 300   # +5 mins for potential job end latency
    logger.log_mq("Enqueuing job late warning message with delay = {} seconds".format(delta_in_s))

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


def _enqueue_cv_git_push(ctx):
    """Places a message upon the new cv terms notification queue.

    """
    # Skip if unnecessary.
    if not ctx.is_simulation_start:
        return
    if not ctx.cv_terms_persisted_to_db and not ctx.cv_terms_new:
        return

    mq_utils.enqueue(mq.constants.MESSAGE_TYPE_CV)


def _enqueue_fe_notification(ctx):
    """Places a message upon the front-end notification queue.

    """
    mq_utils.enqueue(mq.constants.MESSAGE_TYPE_FE, {
        "event_type": u"simulation_start" if ctx.is_simulation_start else "job_start",
        "cv_terms": ctx.cv_terms_for_fe,
        "job_uid": unicode(ctx.job_uid),
        "simulation_uid": ctx.active_simulation.uid if ctx.is_simulation_start else ctx.simulation_uid
    })
