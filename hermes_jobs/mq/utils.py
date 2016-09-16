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


def invoke(agent_type, tasks, error_tasks, ctx):
    """Invokes a set of message queue tasks and handles errors.

    :param str: MQ agent type.
    :param list tasks: A set of tasks.
    :param list error_tasks: A set of error tasks.
    :param object ctx: Task processing context object.

    """
    def  _get(taskset):
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


    def _invoke(task, err=None):
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

    # Execute tasks.
    for task in _get(tasks):
        try:
            _invoke(task)
        # ... error tasks.
        except Exception as err:
            err_msg = "{0} :: {1} :: {2} :: {3}.".format(agent_type, task, type(err), err)
            logger.log_mq_error(err_msg)
            try:
                for error_task in _get(error_tasks):
                    print agent_type, "ERROR TASK", error_task
                    _invoke(error_task, err)
            except:
                pass
            # Escape out of main loop.
            break
        # ... abort tasks.
        else:
            try:
                if ctx.abort == True:
                    break
            except AttributeError:
                pass
