# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types.py
   :platform: Unix
   :synopsis: Prodiguer monitoring db schema types.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import hashlib

from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy import Unicode

from prodiguer.db.pgres.entity import Entity



# Database schema.
SCHEMA = 'monitoring'


class EnvironmentMetric(Entity):
    """Simulation environment metric (OS performance at compute node).

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_environment_metric'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Attributes.
    simulation_uid = Column(Unicode(63), nullable=False)
    job_uid = Column(Unicode(63), nullable=False)
    action_name = Column(Unicode(511), nullable=False)
    action_timestamp = Column(DateTime, nullable=False)
    dir_to = Column(Unicode(4096), nullable=False)
    dir_from = Column(Unicode(4096), nullable=False)
    duration_ms = Column(Integer, nullable=False)
    size_mb = Column(Float, nullable=False)
    throughput_mb_s = Column(Float, nullable=False)


class Job(Entity):
    """History of job state events.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_job'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Attributes.
    simulation_uid = Column(Unicode(63))
    job_uid = Column(Unicode(63), nullable=False, unique=True)
    accounting_project = Column(Unicode(511))
    execution_start_date = Column(DateTime)
    expected_execution_end_date = Column(DateTime)
    execution_end_date = Column(DateTime)
    is_error = Column(Boolean, default=False)
    typeof = Column(Unicode(63))
    was_late = Column(Boolean)


    def set_was_late_flag(self):
        """Sets value of was_late flag based upon execution status.

        """
        if self.was_late is not None or \
           self.expected_execution_end_date is None or \
           self.execution_end_date is None:
            return

        self.was_late = self.execution_end_date > self.expected_execution_end_date


class Simulation(Entity):
    """A simulation being run in order to test a climate model against an experiment.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Attributes.
    accounting_project = Column(Unicode(511))
    activity = Column(Unicode(127))
    activity_raw = Column(Unicode(127))
    compute_node = Column(Unicode(127))
    compute_node_raw = Column(Unicode(127))
    compute_node_login = Column(Unicode(127))
    compute_node_login_raw = Column(Unicode(127))
    compute_node_machine = Column(Unicode(127))
    compute_node_machine_raw = Column(Unicode(127))
    experiment = Column(Unicode(127))
    experiment_raw = Column(Unicode(127))
    hashid = Column(Unicode(63))
    is_error = Column(Boolean, default=False)
    is_obsolete = Column(Boolean, default=False)
    model = Column(Unicode(127))
    model_raw = Column(Unicode(127))
    space = Column(Unicode(127))
    space_raw = Column(Unicode(127))
    name = Column(Unicode(511))
    ensemble_member = Column(Unicode(15))
    execution_start_date = Column(DateTime)
    execution_end_date = Column(DateTime)
    output_start_date = Column(DateTime)
    output_end_date = Column(DateTime)
    parent_simulation_name = Column(Unicode(511))
    parent_simulation_branch_date = Column(DateTime)
    try_id = Column(Integer, nullable=False, default=1)
    uid = Column(Unicode(63), nullable=False, unique=True)

    @property
    def is_restart(self):
        """Returns flag indicating whether this is a restarted simulation or not.

        """
        return self.try_id > 1

    def get_hashid(self):
        """Returns the computed hash id for a simulation.

        """
        hashid = "".join([
            self.activity,
            self.compute_node,
            self.compute_node_login,
            self.compute_node_machine,
            self.experiment,
            self.model,
            self.space,
            self.name
            ])

        return unicode(hashlib.md5(hashid).hexdigest())


class SimulationConfiguration(Entity):
    """Simulation configuration cards.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation_configuration'
    __table_args__ = (
        {'schema':SCHEMA}
    )

    # Attributes.
    simulation_uid = Column(Unicode(63), nullable=False)
    card = Column(Text, nullable=True)
    card_encoding = Column(Unicode(63), nullable=True, default=u"utf-8")
    card_mime_type = Column(Unicode(63),
                            nullable=True,
                            default=u"application/base64")
