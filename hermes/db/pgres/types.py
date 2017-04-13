# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.types.py
   :platform: Unix
   :synopsis: Hermes db types.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from hermes.db.pgres.types_conso import Allocation
from hermes.db.pgres.types_conso import Consumption
from hermes.db.pgres.types_conso import CPUState
from hermes.db.pgres.types_conso import OccupationStore
from hermes.db.pgres.types_cv import ControlledVocabularyTerm
from hermes.db.pgres.types_monitoring import EnvironmentMetric
from hermes.db.pgres.types_monitoring import Job
from hermes.db.pgres.types_monitoring import JobPeriod
from hermes.db.pgres.types_monitoring import Simulation
from hermes.db.pgres.types_monitoring import SimulationConfiguration
from hermes.db.pgres.types_mq import Message
from hermes.db.pgres.types_mq import MessageEmail
from hermes.db.pgres.types_mq import MessageEmailStats
from hermes.db.pgres.types_superviseur import Supervision



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
