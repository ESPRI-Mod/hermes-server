# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types.py
   :platform: Unix
   :synopsis: Prodiguer db types.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime

from sqlalchemy import Column
from sqlalchemy import DateTime

from prodiguer.db.pgres.types_conso import Allocation
from prodiguer.db.pgres.types_conso import Consumption
from prodiguer.db.pgres.types_conso import OccupationStore
from prodiguer.db.pgres.types_cv import ControlledVocabularyTerm
from prodiguer.db.pgres.types_monitoring import Command
from prodiguer.db.pgres.types_monitoring import EnvironmentMetric
from prodiguer.db.pgres.types_monitoring import Job
from prodiguer.db.pgres.types_monitoring import Simulation
from prodiguer.db.pgres.types_monitoring import SimulationConfiguration
from prodiguer.db.pgres.types_mq import Message
from prodiguer.db.pgres.types_mq import MessageEmail
from prodiguer.db.pgres.types_superviseur import Supervision



# Set of supported model schemas.
SCHEMAS = {'conso', 'cv', 'monitoring', 'mq', 'superviseur'}


# Set of supported model types.
SUPPORTED = TYPES = [
    # ... conso types
    Allocation,
    Consumption,
    OccupationStore,
    # ... cv types
    ControlledVocabularyTerm,
    # ... monitoring types
    Command,
    EnvironmentMetric,
    Job,
    Simulation,
    SimulationConfiguration,
    # ... mq types
    Message,
    MessageEmail,
    # ... superviseur types
    Supervision
]

# Extend type with other fields.
for entity_type in SUPPORTED:
    entity_type.row_create_date = \
        Column(DateTime, nullable=False, default=datetime.datetime.utcnow)
    entity_type.row_update_date = \
        Column(DateTime, onupdate=datetime.datetime.utcnow)
