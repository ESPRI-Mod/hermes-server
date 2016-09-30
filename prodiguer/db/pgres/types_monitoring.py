# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.types.py
   :platform: Unix
   :synopsis: Hermes monitoring database tables.

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
from sqlalchemy import UniqueConstraint

from prodiguer.db.pgres.entity import Entity
from prodiguer.cv.constants import EXECUTION_STATE_COMPLETE
from prodiguer.cv.constants import EXECUTION_STATE_ERROR
from prodiguer.cv.constants import EXECUTION_STATE_QUEUED
from prodiguer.cv.constants import EXECUTION_STATE_RUNNING


# Database schema.
_SCHEMA = 'monitoring'


class Job(Entity):
    """History of job state events.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_job'
    __table_args__ = (
        {'schema':_SCHEMA}
    )

    # Attributes.
    accounting_project = Column(Unicode(511))
    execution_state = Column(Unicode(1), default=EXECUTION_STATE_QUEUED)
    execution_end_date = Column(DateTime)
    execution_start_date = Column(DateTime)
    is_compute_end = Column(Boolean, default=False)
    is_error = Column(Boolean, default=False)
    is_im = Column(Boolean, default=False)
    job_uid = Column(Unicode(63), nullable=False, unique=True)
    post_processing_component = Column(Unicode(63))
    post_processing_date = Column(DateTime)
    post_processing_dimension = Column(Unicode(63))
    post_processing_file = Column(Unicode(127))
    post_processing_name = Column(Unicode(63))
    scheduler_id = Column(Unicode(255))
    simulation_uid = Column(Unicode(63))
    typeof = Column(Unicode(63))
    warning_delay = Column(Integer)
    submission_path = Column(Unicode(2047))


    def get_execution_state(self):
        """Returns current derived execution status.

        """
        if self.execution_start_date and not self.execution_end_date:
            return EXECUTION_STATE_RUNNING
        if self.execution_end_date and self.is_error:
            return EXECUTION_STATE_ERROR
        if self.execution_end_date and not self.is_error:
            return EXECUTION_STATE_COMPLETE
        return EXECUTION_STATE_QUEUED


class JobPeriod(Entity):
    """History of job period related events.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_job_period'
    __table_args__ = (
        UniqueConstraint(
            'job_uid',
            'period_id',
            'period_date_begin',
            'period_date_end'
            ),
        {'schema':_SCHEMA}
    )

    # Attributes.
    simulation_uid = Column(Unicode(63), nullable=False)
    job_uid = Column(Unicode(63), nullable=False)
    period_id = Column(Integer, nullable=False)
    period_date_begin = Column(Integer, nullable=False)
    period_date_end = Column(Integer, nullable=False)


class Simulation(Entity):
    """A simulation being run in order to test a climate model against an experiment.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation'
    __table_args__ = (
        {'schema':_SCHEMA}
    )

    # Attributes.
    accounting_project = Column(Unicode(511))
    compute_node = Column(Unicode(127))
    compute_node_raw = Column(Unicode(127))
    compute_node_login = Column(Unicode(127))
    compute_node_login_raw = Column(Unicode(127))
    compute_node_machine = Column(Unicode(127))
    compute_node_machine_raw = Column(Unicode(127))
    execution_end_date = Column(DateTime)
    execution_start_date = Column(DateTime)
    experiment = Column(Unicode(127))
    experiment_raw = Column(Unicode(127))
    hashid = Column(Unicode(63))
    is_error = Column(Boolean, default=False)
    is_obsolete = Column(Boolean, default=False)
    model = Column(Unicode(127))
    model_raw = Column(Unicode(127))
    name = Column(Unicode(511))
    output_end_date = Column(DateTime)
    output_start_date = Column(DateTime)
    space = Column(Unicode(127))
    space_raw = Column(Unicode(127))
    try_id = Column(Integer, nullable=False, default=1)
    uid = Column(Unicode(63), nullable=False, unique=True)

    # libIGCM paths.
    submission_path = Column(Unicode(2047))
    archive_path = Column(Unicode(2047))
    storage_path = Column(Unicode(2047))
    storage_small_path = Column(Unicode(2047))

    # Obsolete ???
    ensemble_member = Column(Unicode(15))
    parent_simulation_name = Column(Unicode(511))
    parent_simulation_branch_date = Column(DateTime)

    @property
    def is_restart(self):
        """Returns flag indicating whether this is a restarted simulation or not.

        """
        return self.try_id > 1

    def get_hashid(self):
        """Returns the computed hash id for a simulation.

        """
        hashid = "".join([
            self.accounting_project or "UNKNOWN",
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
        {'schema':_SCHEMA}
    )

    # Attributes.
    simulation_uid = Column(Unicode(63), nullable=False)
    card = Column(Text, nullable=True)
    card_encoding = Column(Unicode(63), nullable=True, default=u"utf-8")
    card_mime_type = Column(Unicode(63),
                            nullable=True,
                            default=u"application/base64")


class EnvironmentMetric(Entity):
    """Simulation environment metric (OS performance at compute node).

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_environment_metric'
    __table_args__ = (
        {'schema':_SCHEMA}
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
