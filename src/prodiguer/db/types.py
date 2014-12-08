# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types.py
   :platform: Unix
   :synopsis: Prodiguer db types.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
# Import utils.
import datetime

from sqlalchemy import (
    Column,
    DateTime
    )

from . import type_utils
from . type_utils import (
    assert_type,
    Entity,
    ControlledVocabularyEntity,
    Convertor,
    metadata,
    parse_attr_value
    )

# Import cnode types.
from .types_cnode import (
    ComputeNode,
    ComputeNodeLogin,
    ComputeNodeMachine,
    Experiment,
    ExperimentGroup,
    Model,
    ModelForcing,
    Simulation,
    SimulationForcing,
    SimulationSpace,
    SimulationState,
    SimulationStateChange,
    NewSimulation,
    NewSimulationStateChange
    )

# Import mq types.
from .types_mq import Message

# Import shared schema
from .types_shared import (
    Activity,
    CvTerm,
    CV_TYPES,
    CV_TYPE_ACTIVITY,
    CV_TYPE_INSTITUTE,
    Institute
    )



__all__ = [
    # ... cnode types
    "ComputeNode",
    "ComputeNodeLogin",
    "ComputeNodeMachine",
    "Experiment",
    "ExperimentGroup",
    "Model",
    "ModelForcing",
    "Simulation",
    "SimulationForcing",
    "SimulationSpace",
    "SimulationState",
    "SimulationStateChange",
    "NewSimulation",
    "NewSimulationStateChange",
    # ... mq types
    "Message",
    # ... shared types
    "Activity",
    "CvTerm",
    "CV_TYPES",
    "CV_TYPE_ACTIVITY",
    "CV_TYPE_INSTITUTE",
    "Institute",
    # ... other
    "Entity",
    "Convertor",
    "metadata",
    "parse_attr_value",
    "assert_type",
    "SCHEMAS",
    "TYPES",
    "CACHEABLE",
    "CV",
    ]



# Set of supported model schemas.
SCHEMAS = ("cnode", "mq", "shared")


# Set of supported model types.
TYPES = type_utils.supported_types = [
    # ... cnode
    ComputeNode,
    ComputeNodeLogin,
    ComputeNodeMachine,
    Experiment,
    ExperimentGroup,
    Model,
    ModelForcing,
    Simulation,
    SimulationForcing,
    SimulationSpace,
    SimulationState,
    SimulationStateChange,
    NewSimulation,
    NewSimulationStateChange,
    # ... mq
    Message,
    # ... shared
    Activity,
    Institute,
    CvTerm
]

# Extend type with other fields.
for type in TYPES:
    type.row_create_date = Column(DateTime, nullable=False, default=datetime.datetime.now)
    type.row_update_date = Column(DateTime, onupdate=datetime.datetime.now)


# Supported cv types - order matters so as to ensure population can take place smoothly.
CV = (
    # ... shared cv's.
    Activity,
    CvTerm,
    Institute,
    # ... cnode cv's.
    ComputeNode,
    ComputeNodeLogin,
    ComputeNodeMachine,
    Model,
    ModelForcing,
    ExperimentGroup,
    Experiment,
    SimulationSpace,
    SimulationState,
    # ... temp cv's.
    Simulation
    )


# Supported cacheable types.
CACHEABLE = (
    # ... cnode
    ComputeNode,
    ComputeNodeLogin,
    ComputeNodeMachine,
    Experiment,
    ExperimentGroup,
    Model,
    SimulationSpace,
    SimulationState,
    # ... shared
    Activity,
    Institute,
    CvTerm,
    )
