# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.utils.runtime.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Runtime utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os

from prodiguer.utils import logger



def get_path_to_repos():
    """Returns path to Prodiguer source code repositories.

    """
    dir_repos = os.path.dirname(__file__)
    for _ in range(3):
        dir_repos = os.path.dirname(dir_repos)

    return dir_repos


def get_path_to_repo(subpaths):
    """Returns path to Prodiguer source code repositories.

    """
    dir_repo = get_path_to_repos()
    for subpath in subpaths:
        dir_repo = os.path.join(dir_repo, subpath)

    return dir_repo


def invoke(tasks, ctx=None, module="**"):
    """Invokes a set of tasks and handles errors.

    :param dict tasks: A set of tasks divided into green/red.
    :param object ctx: Task processing context object.

    """
    def _invoke(task, err=None):
        """Invokes an individual task."""
        if ctx:
            if err:
                task(ctx, err)
            else:
                task(ctx)
        else:
            if err:
                task(err)
            else:
                task()

    # Execute green tasks.
    for task in tasks["green"]:
        try:
            _invoke(task)
        # Execute red tasks.
        except Exception as err:
            try:
                logger.log_error(err, module)
                for error_task in tasks["red"]:
                    _invoke(error_task, err)
            except:
                pass
            # N.B. break out of green line.
            break


def invoke_mq(agent_type, tasks, error_tasks, ctx):
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
