# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types.py
   :platform: Unix
   :synopsis: Prodiguer db types.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


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
    SimulationMessage,
    SimulationMetric,
    SimulationMetricGroup,
    SimulationSpace,
    SimulationState
    )

# Import dnode types.
from .types_dnode import (
    DataNode,
    DataServer,
    DataServerType,
    DRSComponent,
    DRSElement,
    DRSElementMapping,
    DRSSchema
    )

# Import mq types.
from .types_mq import (
    Message,
    MessageApplication,
    MessageProducer,
    MessageType
    )

# Import shared schema
from .types_shared import (
    Activity,
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
    "SimulationMessage",
    "SimulationMetric",
    "SimulationMetricGroup",
    "SimulationSpace",
    "SimulationState",
    # ... dnode types
    "DataNode",
    "DataServer",
    "DataServerType",
    "DRSComponent",
    "DRSElement",
    "DRSElementMapping",
    "DRSSchema",
    # ... mq types
    "Message",
    "MessageApplication",
    "MessageProducer",
    "MessageType",
    # ... shared types
    "Activity",
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
SCHEMAS = ("cnode", "dnode", "mq", "shared")


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
    SimulationMessage,
    SimulationMetric,
    SimulationMetricGroup,
    SimulationSpace,
    SimulationState,
    # ... dnode
    DataNode,
    DataServer,
    DataServerType,
    DRSComponent,
    DRSElement,
    DRSElementMapping,
    DRSSchema,
    # ... mq
    Message,
    MessageApplication,
    MessageProducer,
    MessageType,
    # ... shared
    Activity,
    Institute,
]

# Extend type with other fields.
for type in TYPES:
    type.row_create_date = Column(DateTime, nullable=False, default=datetime.datetime.now)
    type.row_update_date = Column(DateTime, onupdate=datetime.datetime.now)


# Supported cv types - order matters so as to ensure population can take place smoothly.
CV = (
    # ... shared cv's.
    Activity,
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
    # ... mq cv's.
    MessageApplication,
    MessageProducer,
    MessageType,
    # ... dnode cv's.
    DataNode,
    DataServerType,
    DataServer,
    DRSSchema,
    DRSComponent,
    DRSElement,
    DRSElementMapping,
    # ... temp cv's.
    Simulation,
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
    # ... mq
    MessageApplication,
    MessageProducer,
    MessageType,
    # ... shared
    Activity,
    Institute,
    )
