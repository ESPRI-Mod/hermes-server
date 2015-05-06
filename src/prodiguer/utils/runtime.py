# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.utils.runtime.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Runtime utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import inspect
import os

from prodiguer.utils import logger



class ProdiguerException(Exception):
    """Default library exception class.

    """

    def __init__(self, msg):
        """Contructor.

        :param msg: Exception message.
        :type msg: str

        """
        self.message = msg() if inspect.isfunction(msg) else str(msg)


    def __str__(self):
        """Returns a string representation.

        """
        return "IPSL PRODIGUER EXCEPTION : {0}".format(repr(self.message))


def get_path_to_repos():
    """Returns path to Prodiguer source code repositories.

    """
    dir_repos = os.path.dirname(__file__)
    for _ in range(4):
        dir_repos = os.path.dirname(dir_repos)

    return dir_repos


def get_path_to_repo(subpaths):
    """Returns path to Prodiguer source code repositories.

    """
    dir_repo = get_path_to_repos()
    for subpath in subpaths:
        dir_repo = os.path.join(dir_repo, subpath)

    return dir_repo


def assert_function(f, msg=None):
    """Asserts that a variable is a function.

    :param f: Variable that should be a function pointer.
    :type f: function

    :param msg: Error message to output if assertion fails.
    :type msg: str or None

    """
    def get_msg():
        return "Function assertion failure."

    if not inspect.isfunction(f):
        throw(get_msg if msg is None else msg)


def assert_var(name, value, type, msg=None):
    """Asserts that a variable is of the expected type.

    :param name: Variable name.
    :type name: str

    :param name: Variable value.
    :type value: object

    :param name: Variable type.
    :type type: class

    :param msg: Error message to output if assertion fails.
    :type msg: str or None

    """
    def get_msg():
        msg = "Parameter '{0}' is of an invalid type (expected type = {1})."
        return msg.format(name, type.__name__)

    if value is None or not isinstance(value, type):
        throw(get_msg if msg is None else msg)


def assert_optional_var(name, value, type, msg=None):
    """Asserts that a variable is of the expected type if it is not None.

    :param name: Variable name.
    :type name: str

    :param name: Variable value.
    :type value: object

    :param name: Variable type.
    :type type: class

    :param msg: Error message to output if assertion fails.
    :type msg: str or None

    """
    if value is not None:
        assert_var(name, value, type, msg)


def assert_iter_item(collection, item, msg=None):
    """Asserts that an item is a member of passed collection.

    :param collection: A collection that should contain the specified item.
    :type collection: iterable

    :param item: An item that should be a collection member.
    :type item: object

    :param msg: Error message to output if assertion fails.
    :type msg: str or None

    """
    def get_msg():
        return "Item not found within collection :: {0}.".format(item)

    assert_iter(collection)
    if not item in collection:
        throw(get_msg if msg is None else msg)


def assert_iter(collection, msg=None):
    """Asserts that an item is a an iterable.

    :param collection: A collection that should be iterable.
    :type collection: iterable

    :param msg: Error message to output if assertion fails.
    :type msg: str or None

    """
    def get_msg():
        return "Collection is not iterable."

    try:
        iter(collection)
    except TypeError:
        throw(get_msg if msg is None else msg)


def assert_typed_iter(collection, type, msg=None):
    """Asserts that each collection member is of the expected type.

    :param collection: A collection.
    :type collection: iterable


    :param type: Type that each collection item should sub-class.
    :type type: class or None

    :param msg: Error message to output if assertion fails.
    :type msg: str or None

    """
    def get_msg():
        msg = "Collection contains items of an invalid type (expected type = {0})."
        return msg.format(type.__name__)

    assert_iter(collection)
    if len([i for i in collection if not isinstance(i, type)]) > 0:
        throw(get_msg if msg is None else msg)


def assert_attr(instance, attr, msg=None):
    """Asserts that passed instance has the passed attribute (i.e. is it a duck ?).

    :param instance: An object instance.
    :type item: object

    :param attr: Name of attribute that instance should contain.
    :type item: str

    :param msg: Error message to output if assertion fails.
    :type msg: str or None

    """
    def get_msg():
        return "Attribute {0} is not found.".format(attr)

    assert_var('instance', instance, object)
    if not hasattr(instance, attr):
        throw(get_msg if msg is None else msg)


def assert_doc(name, value, msg=None):
    """Asserts thay passed variable is a pyesdoc object instance.

    :param name: Variable name.
    :type name: str

    :param value: Variable value.
    :type value: object

    :param msg: Error message to output if assertion fails.
    :type msg: str or None

    """
    def get_msg():
        return msg if msg is not None else \
               "{0} is not a pyesdoc type instance".format(name)

    assert_var(name, value, object, msg=get_msg)
    assert_attr(value, 'doc_info', msg="Document meta-information is missing")


def assert_params(params, rules):
    """Performs a set of assertions over a parameter dictionary.

    :param params: Dictionary or input parameters.
    :type params: dict

    :param rules: Set of assertion rules.
    :type rules: list

    """
    for rule in rules:
        # Unpack rule.
        name, white_list = rule

        # Assert param is specified.
        if not params.has_key(name) or not len(str(params[name])):
            throw("Parameter {0} is unspecified.".format(name))

        # Assert param value is in constrained list.
        if len(white_list):
            if params[name] not in white_list:
                throw("Parameter {0} is invalid.".format(name))


def throw(msg):
    """Throws an error.

    :param msg: Error message.
    :type msg: str

    """
    logger.log("UNHANDLED ERROR :: {0}".format(msg))

    raise ProdiguerException(msg)


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


def invoke_mq(agent_type, tasks, error_tasks=None, ctx=None):
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
                if ctx.abort:
                    break
            except AttributeError:
                pass
