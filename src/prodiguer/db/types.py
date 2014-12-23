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

from prodiguer.db import type_utils
from prodiguer.db.type_utils import (
    assert_type,
    Entity,
    Convertor,
    metadata
    )
from prodiguer.db.types_monitoring import (
    SimulationForcing,
    Simulation,
    SimulationStateChange
    )
from prodiguer.db.types_mq import Message



__all__ = [
    # ... type related
    "SimulationForcing",
    "Simulation",
    "SimulationStateChange",
    "Message",
    # ... other
    "Entity",
    "Convertor",
    "metadata",
    "assert_type",
    "SCHEMAS",
    "TYPES",
    "CACHEABLE"
    ]



# Set of supported model schemas.
SCHEMAS = ("monitoring", "mq")


# Set of supported model types.
TYPES = type_utils.supported_types = [
    Message,
    Simulation,
    SimulationForcing,
    SimulationStateChange
]

# Extend type with other fields.
for entity_type in TYPES:
    entity_type.row_create_date = \
        Column(DateTime, nullable=False, default=datetime.datetime.now)
    entity_type.row_update_date = \
        Column(DateTime, onupdate=datetime.datetime.now)


# Supported cacheable types.
CACHEABLE = []
