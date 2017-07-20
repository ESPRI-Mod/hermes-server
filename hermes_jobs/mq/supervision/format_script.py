# -*- coding: utf-8 -*-

"""
.. module:: supervisor_format_script.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Formats supervision scripts in readiness for dispatch.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from hermes_jobs.mq import utils
from hermes import mq
from hermes.db import pgres as db
from hermes.db.pgres.dao_monitoring import retrieve_job
from hermes.db.pgres.dao_monitoring import retrieve_latest_job_period
from hermes.db.pgres.dao_monitoring import retrieve_latest_job_period_counter
from hermes.db.pgres.dao_monitoring import retrieve_simulation
from hermes.db.pgres.dao_superviseur import retrieve_supervision
from hermes.utils import config
from hermes.utils import logger
from hermes.utils import mail
import superviseur



_EMAIL_SUBJECT = u"HERMES-SUPERVISOR :: COMPUTE JOB FAILURE :: USER={}; JOB={}; MACHINE={}."

# Operator email body template.
_EMAIL_BODY = u"""Dear HERMES platform user {},

The following issue has been detected:

Machine:\t\t\t{}
Login/User:\t\t{}
Job name:\t\t{}
Job number:\t\t{}

A compute job failed within the period {} ({}-{}) for the {}{} time.  Jobs that fail more than {} times within the same output period require your attention.

Further information:

    https://hermes.ipsl.upmc.fr/static/simulation.detail.html?uid={}
    https://hermes.ipsl.upmc.fr/static/simulation.monitoring.html?login={}

Best regards,

The HERMES Platform

"""


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack,
        _set_data,
        _authorize,
        _verify,
        _format,
        _enqueue_script_dispatch,
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.job = None
        self.job_period = None
        self.job_uid = None
        self.simulation = None
        self.supervision = None
        self.supervision_id = None
        self.script = None
        self.user = None


def _unpack(ctx):
    """Unpacks message being processed.

    """
    ctx.job_uid = ctx.content['job_uid']
    ctx.simulation_uid = ctx.content['simulation_uid']
    ctx.supervision_id = int(ctx.content['supervision_id'])
    ctx.retry_count = ctx.content.get('retry_count', 0)


def _on_simulation_not_found(ctx):
    """Simulation not found event handler.

    """
    # Abort further processing.
    ctx.abort = True

    # Warn operator.
    msg = "Supervision not feasible: simulation not found: sim-uid={}.  Possible 8888 scenario."
    msg = msg.format(ctx.simulation_uid)
    logger.log_mq_warning(msg)


def _on_simulation_login_null(ctx):
    """Simulation login null event handler.

    """
    # Abort further processing.
    ctx.abort = True

    # Warn operator.
    msg = "Supervision not possible: simulation login unspecified: sim-uid={}"
    msg = msg.format(ctx.simulation_uid)
    logger.log_mq_warning(msg)

    # Maximum retries reached.
    if ctx.retry_count >= config.apps.superviseur.maxScriptFormattingAttempts:
        msg = "Supervision formatting not possible: maximum retries exceeded: sim-uid={}"
        msg = msg.format(ctx.simulation_uid)
        logger.log_mq_warning(msg)
        return

    # Warn operator.
    msg = "Supervision formatting requeued: sim-uid={}"
    msg = msg.format(ctx.simulation_uid)
    logger.log_mq_warning(msg)

    # Requeue (delayed).
    utils.enqueue(
        mq.constants.MESSAGE_TYPE_8100,
        delay_in_ms=config.apps.superviseur.scriptReformattingDelayInSeconds * 1000,
        exchange=mq.constants.EXCHANGE_HERMES_SECONDARY_DELAYED,
        payload={
            "job_uid": ctx.job_uid,
            "simulation_uid": ctx.simulation_uid,
            "supervision_id": ctx.supervision_id,
            "retry_count": ctx.retry_count + 1
        })


def _set_data(ctx):
    """Sets data also required by script formatter.

    """
    # Escape if simulation purged.
    ctx.simulation = retrieve_simulation(ctx.simulation_uid)
    if ctx.simulation is None:
        _on_simulation_not_found(ctx)
        return

    # Escape if 0000 message has not yet been received.
    if ctx.simulation.compute_node_login is None:
        _on_simulation_login_null(ctx)
        return

    ctx.job = retrieve_job(ctx.job_uid)
    ctx.job_period = retrieve_latest_job_period(ctx.simulation_uid)
    ctx.job_period_counter = retrieve_latest_job_period_counter(ctx.simulation_uid)
    ctx.supervision = retrieve_supervision(ctx.supervision_id)


def _authorize(ctx):
    """Verifies that the user has authorized supervision.

    """
    try:
        ctx.user = superviseur.authorize(ctx.simulation.compute_node_login)
    except UserWarning as err:
        logger.log_mq_warning("Supervision authorization failure: {}".format(err))
        ctx.abort = True


def _get_email_subject(ctx):
    """Returns subject of email to be sent to user.

    """
    return _EMAIL_SUBJECT.format(
        ctx.user.login,
        ctx.job.scheduler_id,
        ctx.simulation.compute_node_machine_raw
        )


def _get_job_failure_count_suffix(count):
    """Returns a count suffix.

    """
    count = int(count)
    if count in [1, 21, 31, 41, 51]:
        return "st"
    if count in [2, 22, 32, 42, 52]:
        return "nd"
    if count in [3, 23, 33, 43, 53]:
        return "rd"
    return "th"


def _get_email_body(ctx):
    """Returns body of email to be sent to user.

    """
    return _EMAIL_BODY.format(
        ctx.user.login,
        ctx.simulation.compute_node_machine_raw.upper(),
        ctx.user.login,
        ctx.job.scheduler_id,
        ctx.simulation.name,
        ctx.job_period.period_id,
        ctx.job_period.period_date_begin,
        ctx.job_period.period_date_end,
        ctx.job_period_counter[1],
        _get_job_failure_count_suffix(ctx.job_period_counter[1]),
        config.apps.superviseur.maxJobPeriodFailures,
        ctx.simulation.uid,
        ctx.user.login
        )


def _verify(ctx):
    """Verifies that the job formatting is required.

    """
    # Accepted if no job period received.
    if ctx.job_period is None:
        return

    # Rejected if job period is undefined.
    if ctx.job_period.period_id is None:
        logger.log_mq_warning("Job period empty")
        ctx.abort = True

    # Rejected if dealing with the first job period.
    elif ctx.job_period.period_id == 1:
        logger.log_mq("Initial job periods do not require supervision")
        ctx.abort = True

    # Rejected if job period failure count exceeds the configurable limit.
    elif ctx.job_period_counter[1] > config.apps.superviseur.maxJobPeriodFailures:
        mail.send_email(config.alerts.emailAddressFrom,
            ctx.user.email,
            _get_email_subject(ctx),
            _get_email_body(ctx)
            )
        logger.log_mq("Too many tries for the last job period, mail sent to user and supervision aborted")
        ctx.abort = True


def _format(ctx):
    """Formats the script for the job to be executed at the compute node.

    """
    # Set formatter parameters.
    params = superviseur.FormatParameters(
        ctx.simulation,
        ctx.job,
        ctx.supervision,
        ctx.user
        )

    # Format script to be dispatched to HPC for execution.
    try:
        ctx.supervision.script = superviseur.format_script(params)
    # ... handle formatting errors
    # TODO define error strategy
    except:
        ctx.abort = True
    else:
        db.session.commit()


def _enqueue_script_dispatch(ctx):
    """Enqueues a script dispatch messages.

    """
    utils.enqueue(mq.constants.MESSAGE_TYPE_8200, {
        "supervision_id": ctx.supervision_id
    })
