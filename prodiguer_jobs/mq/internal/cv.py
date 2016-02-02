# -*- coding: utf-8 -*-

"""
.. module:: cv.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Pushes new CV terms to GitHub repo.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer_jobs.mq import utils



# Prodiguer shell command to be executed.
_SHELL_CMD = 'cv-git-push'


def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return _push_to_remote_repo


def _push_to_remote_repo(ctx):
    """Invokes API endpoint.

    """
    utils.exec_shell_command(_SHELL_CMD)
