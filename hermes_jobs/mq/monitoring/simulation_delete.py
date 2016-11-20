# -*- coding: utf-8 -*-

"""
.. module:: simulation_delete.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes messages instructing platform to delete simulation.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>

"""
from prodiguer import mq
from prodiguer.db.pgres import dao
from prodiguer.db.pgres import types



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack_content,
        _delete_environment_metrics,
        _delete_jobs,
        _delete_job_periods,
        _delete_simulation,
        _delete_simulation_configuration,
        _delete_supervision
        )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.simulation_uid = None


def _unpack_content(ctx):
    """Unpacks message being processed.

    """
    ctx.simulation_uid = ctx.content['simuid']


def _delete_environment_metrics(ctx):
    """Deletes simulation environment metrics from dB.

    """
    dao.delete_by_facet(types.EnvironmentMetric,
                        types.EnvironmentMetric.simulation_uid == ctx.simulation_uid)


def _delete_jobs(ctx):
    """Deletes associated jobs from dB.

    """
    dao.delete_by_facet(types.Job,
                        types.Job.simulation_uid == ctx.simulation_uid)


def _delete_job_periods(ctx):
    """Deletes associated job periods from dB.

    """
    dao.delete_by_facet(types.JobPeriod,
                        types.JobPeriod.simulation_uid == ctx.simulation_uid)


def _delete_simulation(ctx):
    """Deletes simulation from dB.

    """
    dao.delete_by_facet(types.Simulation,
                        types.Simulation.simulation_uid == ctx.simulation_uid)


def _delete_simulation_configuration(ctx):
    """Deletes simulation configuration from dB.

    """
    dao.delete_by_facet(types.SimulationConfiguration,
                        types.SimulationConfiguration.simulation_uid == ctx.simulation_uid)


def _delete_supervision(ctx):
    """Deletes simulation supervision information from dB.

    """
    dao.delete_by_facet(types.Supervision,
                        types.Supervision.simulation_uid == ctx.simulation_uid)

