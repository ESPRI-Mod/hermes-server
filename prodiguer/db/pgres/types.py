# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.types.py
   :platform: Unix
   :synopsis: Hermes db types.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres.types_conso import Allocation
from prodiguer.db.pgres.types_conso import Consumption
from prodiguer.db.pgres.types_conso import CPUState
from prodiguer.db.pgres.types_conso import OccupationStore
from prodiguer.db.pgres.types_cv import ControlledVocabularyTerm
from prodiguer.db.pgres.types_monitoring import EnvironmentMetric
from prodiguer.db.pgres.types_monitoring import Job
from prodiguer.db.pgres.types_monitoring import JobPeriod
from prodiguer.db.pgres.types_monitoring import Simulation
from prodiguer.db.pgres.types_monitoring import SimulationConfiguration
from prodiguer.db.pgres.types_mq import Message
from prodiguer.db.pgres.types_mq import MessageEmail
from prodiguer.db.pgres.types_mq import MessageEmailStats
from prodiguer.db.pgres.types_superviseur import Supervision



# Set of supported model schemas.
SCHEMAS = {'conso', 'cv', 'monitoring', 'mq', 'superviseur'}


# Set of supported model types.
SUPPORTED = TYPES = [
    # ... conso types
    Allocation,
    Consumption,
    CPUState,
    OccupationStore,
    # ... cv types
    ControlledVocabularyTerm,
    # ... monitoring types
    EnvironmentMetric,
    Job,
    JobPeriod,
    Simulation,
    SimulationConfiguration,
    # ... mq types
    Message,
    MessageEmail,
    MessageEmailStats,
    # ... superviseur types
    Supervision
]
