# -*- coding: utf-8 -*-

"""
.. module:: consumer_utils.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Message consumer utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer import mq
from prodiguer.db import pgres as db
from prodiguer.utils import logger
from prodiguer import __version__ as HERMES_VERSION



def enqueue(
    message_type,
    payload=None,
    user_id=mq.constants.USER_HERMES,
    producer_id = mq.constants.PRODUCER_HERMES,
    producer_version = HERMES_VERSION,
    exchange=mq.constants.EXCHANGE_HERMES_SECONDARY,
    delay_in_ms=None
    ):
    """Enqueues a message upon MQ server.

    :param str message_type: Message type, e.g. 0000.
    :param dict payload: Message payload.
    :param str user_id: MQ user id, e.g. hermes-mq-user.
    :param str producer_id: MQ producer identifier, e.g. libIGCM.
    :param str producer_version: MQ server producer version, e.g. 2.7.
    :param str exchange: MQ exchange, e.g. x-secondary.
    :param int delay_in_ms: Delay (in milliseconds) before message is routed.

    """
    def _get_msg_props():
        """Returns AMPQ message properties.

        """
        return mq.utils.create_ampq_message_properties(
            user_id=user_id,
            producer_id=producer_id,
            producer_version=producer_version,
            message_type=message_type,
            delay_in_ms=delay_in_ms
            )

    def _yield_message():
        """Yeild a mesage to be enqueued.

        """
        yield mq.Message(_get_msg_props(), payload or {})

    mq.produce(_yield_message)


def _invoke(task, ctx, err=None):
    """Invokes an individual task.

    """
    if ctx and err:
        task(ctx, err)
    elif ctx:
        task(ctx)
    elif err:
        task(err)
    else:
        task()


def  _get_taskset(taskset):
    """Gets formatted tasks in readiness for execution.

    """
    if taskset is None:
        return []
    else:
        try:
            iter(taskset)
        except TypeError:
            return [taskset]
        else:
            return taskset


def _on_invoke_complete(ctx, err=None):
    """Set message processing error either after sucessful or failed message processing.

    """
    # Escape if the context is a message delegator.
    if hasattr(ctx, "is_delegator"):
        return

    # Update message processing error.
    if hasattr(ctx, "msg"):
        if err is not None:
            err = unicode(err)
        if ctx.msg.processing_error != err:
            ctx.msg.processing_error = err
            db.session.update(ctx.msg)


def _on_invoke_error(agent_type, error_tasks, ctx, task, err):
    """Invocation error handler.

    """
    # Log.
    err_msg = "{} :: {} :: {} :: {}.".format(agent_type, task, type(err), err)
    logger.log_mq_error(err_msg)

    # Invoke error tasks (apply sub-error shielding).
    try:
        for error_task in _get_taskset(error_tasks):
            _invoke(error_task, ctx, err)
    except Exception as err:
        try:
            err_msg = "SUB-ERROR !! :: {} :: {} :: {} :: {}.".format(agent_type, error_task, type(err), err)
            logger.log_mq_error(err_msg)
        except:
            pass


def invoke(agent_type, tasks, error_tasks, ctx):
    """Invokes a set of message queue tasks and handles errors.

    :param str: MQ agent type.
    :param list tasks: A set of tasks.
    :param list error_tasks: A set of error tasks.
    :param object ctx: Task processing context object.

    """
    err = None

    # Invoke taskset.
    for task in _get_taskset(tasks):
        try:
            _invoke(task, ctx)
        except Exception as err:
            _on_invoke_error(agent_type, error_tasks, ctx, task, err)
            break
        else:
            try:
                if ctx.abort == True:
                    break
            except AttributeError:
                pass

    # Clean up.
    _on_invoke_complete(ctx, err)
