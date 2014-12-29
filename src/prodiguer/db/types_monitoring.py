# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_monitoring.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of monitoring db types.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime, uuid

from sqlalchemy import (
    Column,
    DateTime,
    Text,
    Unicode
    )

from prodiguer.db.type_utils import Entity



class Simulation(Entity):
    """A simulation being run in order to test a climate model against an experiment.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation'
    __table_args__ = (
        {'schema':'monitoring'}
    )

    # Attributes.
    activity = Column(Unicode(127))
    compute_node = Column(Unicode(127))
    compute_node_login = Column(Unicode(127))
    compute_node_machine = Column(Unicode(127))
    execution_state = Column(Unicode(127))
    experiment = Column(Unicode(127))
    model = Column(Unicode(127))
    space = Column(Unicode(127))
    name = Column(Unicode(511), nullable=False, unique=True)
    ensemble_member = Column(Unicode(15))
    execution_start_date = Column(DateTime, nullable=False, default=datetime.datetime.now)
    execution_end_date = Column(DateTime)
    output_start_date = Column(DateTime)
    output_end_date = Column(DateTime)
    parent_simulation_name = Column(Unicode(511))
    parent_simulation_branch_date = Column(DateTime)
    uid = Column(Unicode(63),
                 nullable=False,
                 unique=True,
                 default=lambda: unicode(uuid.uuid4()))


class SimulationStateChange(Entity):
    """History of simulation status changes.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation_state_change'
    __table_args__ = (
        {'schema':'monitoring'}
    )

    # Attributes.
    simulation_uid = Column(Unicode(63), nullable=False)
    state = Column(Unicode(127))
    timestamp = Column(DateTime, nullable=False)
    info = Column(Unicode(63), nullable=False)


class SimulationConfiguration(Entity):
    """Simulation configuration cards.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation_configuration'
    __table_args__ = (
        {'schema':'monitoring'}
    )

    # Attributes.
    simulation_uid = Column(Unicode(63), nullable=False)
    card = Column(Text, nullable=True)
    card_encoding = Column(Unicode(63), nullable=True, default=u"utf-8")
    card_mime_type = Column(Unicode(63),
                            nullable=True,
                            default=u"application/base64")


class SimulationForcing(Entity):
    """The simulation forcing, i.e. related to configuration.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation_forcing'
    __table_args__ = (
        {'schema':'monitoring'}
    )

    # Foreign keys.
    simulation_uid = Column(Unicode(63), nullable=False)
    forcing = Column(Unicode(127))
