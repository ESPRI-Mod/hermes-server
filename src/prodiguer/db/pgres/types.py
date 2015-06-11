# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types.py
   :platform: Unix
   :synopsis: Prodiguer db types.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime
import hashlib
import uuid

from sqlalchemy import BigInteger
from sqlalchemy import Boolean
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy import Unicode
from sqlalchemy import UniqueConstraint

from prodiguer.db.pgres import type_utils
from prodiguer.db.pgres.type_utils import Entity



# Set of supported model schemas.
_SCHEMA_CV = 'cv'
_SCHEMA_MONITORING = 'monitoring'
_SCHEMA_MQ = 'mq'
SCHEMAS = (_SCHEMA_CV, _SCHEMA_MONITORING, _SCHEMA_MQ)


class ControlledVocabularyTerm(Entity):
    """Represents a CV term.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_cv_term'
    __table_args__ = (
        UniqueConstraint('typeof' ,'name'),
        {'schema':_SCHEMA_CV}
    )

    # Attributes.
    typeof = Column(Unicode(127), nullable=False)
    name = Column(Unicode(127), nullable=False)
    display_name = Column(Unicode(127))
    synonyms = Column(Unicode(1023))


class Simulation(Entity):
    """A simulation being run in order to test a climate model against an experiment.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation'
    __table_args__ = (
        {'schema':_SCHEMA_MONITORING}
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


class Job(Entity):
    """History of job state events.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_job'
    __table_args__ = (
        {'schema':_SCHEMA_MONITORING}
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


class SimulationConfiguration(Entity):
    """Simulation configuration cards.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation_configuration'
    __table_args__ = (
        {'schema':_SCHEMA_MONITORING}
    )

    # Attributes.
    simulation_uid = Column(Unicode(63), nullable=False)
    card = Column(Text, nullable=True)
    card_encoding = Column(Unicode(63), nullable=True, default=u"utf-8")
    card_mime_type = Column(Unicode(63),
                            nullable=True,
                            default=u"application/base64")


class EnvironmentMetric(Entity):
    """Simulation environment metrics of OS performance.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_environment_metric'
    __table_args__ = (
        {'schema':_SCHEMA_MONITORING}
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


class Message(Entity):
    """Represents a message flowing through the MQ platform.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message'
    __table_args__ = (
        {'schema':_SCHEMA_MQ}
    )

    # Attributes.
    app_id = Column(Unicode(63), nullable=False)
    producer_id = Column(Unicode(63), nullable=False)
    type_id = Column(Unicode(63), nullable=False)
    user_id = Column(Unicode(63), nullable=False)
    uid = Column(Unicode(63),
                 nullable=False,
                 unique=True,
                 default=unicode(uuid.uuid4()))
    correlation_id_1 = Column(Unicode(63), nullable=True)
    correlation_id_2 = Column(Unicode(63), nullable=True)
    correlation_id_3 = Column(Unicode(63), nullable=True)
    timestamp = Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    timestamp_raw = Column(Unicode(63), nullable=True)
    timestamp_precision = Column(Unicode(7), nullable=False, default=u"ms")
    content_encoding = Column(Unicode(63), nullable=True, default=u"utf-8")
    content_type = Column(Unicode(63),
                          nullable=True,
                          default=u"application/json")
    content = Column(Text, nullable=True)


class MessageEmail(Entity):
    """Represents an email received from a computing centre.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_message_email'
    __table_args__ = (
        {'schema':_SCHEMA_MQ}
    )

    # Attributes.
    uid = Column(BigInteger, nullable=False, unique=True)


# Set of supported model types.
TYPES = type_utils.supported_types = [
    ControlledVocabularyTerm,
    EnvironmentMetric,
    Job,
    Message,
    MessageEmail,
    Simulation,
    SimulationConfiguration
]

# Extend type with other fields.
for entity_type in TYPES:
    entity_type.row_create_date = \
        Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    entity_type.row_update_date = \
        Column(DateTime, onupdate=datetime.datetime.utcnow)
