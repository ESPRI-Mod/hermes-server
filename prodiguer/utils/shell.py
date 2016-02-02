# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.utils.shell.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Utility functions for interacting with prodiguer-shell.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os
import subprocess



def exec_command(cmd):
    """Executes a prodiguer-shell command.

    :param str cmd: Prodiguer shell command to be executed.

    """
    cmd_type = cmd.split("-")[0]
    cmd_name = "_".join(cmd.split("-")[1:])
    script = os.getenv("PRODIGUER_HOME")
    script = os.path.join(script, "bash")
    script = os.path.join(script, cmd_type)
    script = os.path.join(script, "{}.sh".format(cmd_name))
    subprocess.call(script, shell=True)


def get_repo_path(subpaths):
    """Returns path to Prodiguer source code repositories.

    :param list subpaths: List of subpath to append to repository path.

    """
    dir_repo = os.path.join(os.getenv("PRODIGUER_HOME"), "repos")
    for subpath in subpaths:
        dir_repo = os.path.join(dir_repo, subpath)

    return dir_repo


