# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_cnode.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of cnode db types.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import datetime

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    Text,
    Unicode,
    UniqueConstraint
    )
from sqlalchemy.orm import (
    backref,
    relationship
    )

from . type_utils import (
    create_fk,
    Entity
    )



# PostGres schema to which the types are attached.
_DB_SCHEMA = 'cnode'



class ComputeNode(Entity):
    """A compute node is an HPC environment hosting several machines upon which climate simulations are executed against climate models.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_compute_node'
    __table_args__ = (
        UniqueConstraint('institute_id', 'name'),
        {'schema' : _DB_SCHEMA}
    )

    # Foreign keys.
    institute_id = create_fk('shared.tbl_institute.id')

    # Relationships.
    machines = relationship("ComputeNodeMachine", backref="compute_node")
    logins = relationship("ComputeNodeLogin", backref="compute_node")
    simulations = relationship("Simulation", backref="compute_node")

    # Attributes.
    name = Column(Unicode(16), nullable=False)
    description = Column(Unicode(127), nullable=False)
    centre_url = Column(Unicode(511), nullable=False)
    dods_server_url = Column(Unicode(511))
    is_active = Column(Boolean, nullable=False, default=True)
    is_default = Column(Boolean, nullable=False, default=False)


class ComputeNodeLogin(Entity):
    """A login under which compute node processes execute.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_compute_node_login'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Foreign keys.
    compute_node_id = create_fk('cnode.tbl_compute_node.id')

    # Attributes.
    login = Column(Unicode(32), nullable=False, unique=True)
    first_name = Column(Unicode(127), nullable=False)
    family_name = Column(Unicode(127), nullable=False)
    email = Column(Unicode(255), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)


    @property
    def name(self):
        """Gets instance name.

        """
        return self.login


    @property
    def full_user_name(self):
        """Gets the full user name derived by concatanation.

        """
        return "{0}, {1} - ({2})".format(self.family_name.upper(), 
                                         self.first_name, 
                                         self.login)


    @classmethod
    def get_default_sort_key(cls):
        """Gets default sort key.

        """
        return lambda x: x.family_name.upper()


class ComputeNodeMachine(Entity):
    """
    A compute machine is a HPC machine upon which climate simulations are executed against climate models.
    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_compute_node_machine'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Foreign keys.
    compute_node_id = create_fk('cnode.tbl_compute_node.id')

    # Attributes.
    name = Column(Unicode(255), nullable=False, unique=True)
    short_name = Column(Unicode(32), nullable=False)
    manafacturer = Column(Unicode(127), nullable=False)
    type = Column(Unicode(32), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)


class Experiment(Entity):
    """An experiment being tested by a model simulation.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_experiment'
    __table_args__ = (
        UniqueConstraint('activity_id', 'name'),
        {'schema' : _DB_SCHEMA}
    )

    # Foreign keys.
    activity_id = create_fk('shared.tbl_activity.id')
    group_id = create_fk('cnode.tbl_experiment_group.id')

    # Attributes.
    name = Column(Unicode(63), nullable=False)
    description = Column(Unicode(511))
    years_per_run = Column(Unicode(16))
    ensemble_size = Column(Unicode(16))
    activity_info_1 = Column(Unicode(127))
    activity_info_2 = Column(Unicode(127))
    activity_info_3 = Column(Unicode(127))
    activity_info_4 = Column(Unicode(127))
    is_active = Column(Boolean, nullable=False, default=True)


    @property
    def FullName(self):
        """Gets the full name derived by concatanation.

        """
        return self.activity.name + u" - " + self.name


    @classmethod
    def get_default_sort_key(cls):
        """Gets default sort key.

        """
        return lambda x: x.FullName.upper()


class ExperimentGroup(Entity):
    """An experiment group.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_experiment_group'
    __table_args__ = (
        UniqueConstraint('activity_id', 'name'),
        {'schema' : _DB_SCHEMA}
    )

    # Foreign keys.
    activity_id = create_fk('shared.tbl_activity.id')

    # Relationships.
    experiments = relationship("Experiment", backref="group")

    # Attributes.
    name = Column(Unicode(63), nullable=False)
    description = Column(Unicode(255), nullable=False)
    short_description = Column(Unicode(127), nullable=False)
    short_description_1 = Column(Unicode(127), nullable=False)
    ordinal_position = Column(Integer, nullable=False, default = 1)
    is_active = Column(Boolean, nullable=False, default=True)


class Model(Entity):
    """A climate model computational engine.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_model'
    __table_args__ = (
        UniqueConstraint('institute_id', 'name'),
        {'schema' : _DB_SCHEMA}
    )

    # Entity relations.
    institute_id = create_fk('shared.tbl_institute.id')

    # Attributes.
    name = Column(Unicode(16), nullable=False, unique=True)
    description = Column(Unicode(127), nullable=False)
    drs_tag_name = Column(Unicode(16), nullable=False, unique=True)
    version = Column(Unicode(8))


class ModelForcing(Entity):
    """Represents a forcing used to drive a climate model simulation.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_model_forcing'
    __table_args__ = (
        UniqueConstraint('activity_id', 'name'),
        {'schema' : _DB_SCHEMA}
    )

    # Foreign keys.
    activity_id = create_fk('shared.tbl_activity.id')

    # Attributes.
    name = Column(Unicode(255), nullable=False, unique=True)
    description = Column(Unicode(127), nullable=False)
    long_description = Column(Unicode(511))


class Simulation(Entity):
    """A simulation being run in order to test a climate model against an experiment.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Relationships.
    forcings = relationship("SimulationForcing", backref="simulation")

    # Foreign keys.
    activity_id = create_fk('shared.tbl_activity.id')
    compute_node_id = create_fk('cnode.tbl_compute_node.id')
    compute_node_login_id = create_fk('cnode.tbl_compute_node_login.id')
    compute_node_machine_id = create_fk('cnode.tbl_compute_node_machine.id')
    execution_state_id = create_fk('cnode.tbl_simulation_state.id')
    experiment_id = create_fk('cnode.tbl_experiment.id')
    model_id = create_fk('cnode.tbl_model.id')
    parent_simulation_id = create_fk('cnode.tbl_simulation.id', nullable=True)
    space_id = create_fk('cnode.tbl_simulation_space.id')

    # Attributes.
    ensemble_member = Column(Unicode(15))
    execution_start_date = Column(DateTime, nullable=False, default=datetime.datetime.now)
    execution_end_date = Column(DateTime)
    name = Column(Unicode(511), nullable=False, unique=True)
    output_start_date = Column(DateTime)
    output_end_date = Column(DateTime)
    parent_simulation_branch_date = Column(DateTime)


    def get_inter_monitor_url(self, cache):
        """Returns URL used to link out to inter-monitor.

        """
        result =  cache.get(ComputeNode, self.compute_node_id).dods_server_url;
        result += '/';
        result += cache.get(ComputeNodeLogin, self.compute_node_login_id).login;
        result += '/';
        result += cache.get(Model, self.model_id).drs_tag_name;
        result += '/';
        result += cache.get(SimulationSpace, self.space_id).name;
        result += '/';
        result += cache.get(Experiment, self.experiment_id).name;
        result += '/';
        result += self.Name;
        
        return result


    def get_monitor_url(self, cache):
        """Returns URL used to link out to monitor.

        """
        result =  self.get_inter_monitor_url(cache);
        result = result.replace('cgi-bin/nph-dods/', '')
        result += u'/MONITORING'

        return result


class SimulationForcing(Entity):
    """The simulation forcing, i.e. related to configuration.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation_forcing'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Foreign keys.
    simulation_id = create_fk('cnode.tbl_simulation.id')
    model_forcing_id = create_fk('cnode.tbl_model_forcing.id')


class SimulationMessage(Entity):
    """A simulation related message.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation_message'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Foreign keys.
    simulation_id = create_fk('cnode.tbl_simulation.id')
    message_id = create_fk('mq.tbl_message.id')


class SimulationMetric(Entity):
    """A simulation metric.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation_metric'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Foreign keys.
    group_id = create_fk('cnode.tbl_simulation_metric_group.id')
    metric = Column(Text, nullable=False)


class SimulationMetricGroup(Entity):
    """A simulation metric group.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation_metric_group'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Attributes.
    name = Column(Unicode(256), nullable=False, unique=True)
    columns = Column(Text, nullable=False)


class SimulationSpace(Entity):
    """Indicates simulation type, i.e. PROD (production), TEST (test), FAIL (failed) ...etc.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_simulation_space'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Attributes.
    name = Column(Unicode(255), nullable=False, unique=True)
    description = Column(Unicode(127), nullable=False)
    is_default = Column(Boolean, nullable=False, default=False)


    @property
    def FullName(self):
        """Gets the full user name derived by concatanation.

        """
        return self.name + u" - " + self.description


class SimulationState(Entity):
    """The state that a simulation may be in, i.e. InProgress, Queued, Error...etc.
    
    """
    # Sqlalchemy directives.
    __tablename__ = 'tbl_simulation_state'
    __table_args__ = (
        {'schema':_DB_SCHEMA}
    )

    # Attributes.
    name = Column(Unicode(16), nullable=False, unique=True)
    description = Column(Unicode(127), nullable=False)
    code =  Column(Integer, nullable=False, unique=True)

    @classmethod
    def get_default_sort_key(cls):
        """Gets default sort key.

        """
        return lambda x: x.code