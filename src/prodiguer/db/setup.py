# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.setup.py
   :platform: Unix
   :synopsis: Initializes database.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os

from sqlalchemy.schema import CreateSchema, DropSchema

from prodiguer import cv
from prodiguer.db import (
    session as db_session,
    types as db_types
    )
from prodiguer.utils import convert, rt



def _init_cv_terms():
    """Initialises set of cv terms.

    """
    rt.log_db("Seeding cv.tbl_cv_term")

    for term in cv.io.read():
        item = db_types.ControlledVocabularyTerm()
        item.typeof = cv.get_type(term)
        item.name = cv.get_name(term)
        item.display_name = cv.get_display_name(term)
        item.synonyms = ", ".join(cv.get_synonyms(term)) or None
        db_session.add(item)


def _init_simulations():
    """Initialises set of simulations.

    """
    rt.log_db("Seeding cnode.tbl_simulation")

    # Set simulations from simulation.json file.
    fpath = os.path.dirname(os.path.abspath(__file__))
    fpath = os.path.join(fpath, "data")
    fpath = os.path.join(fpath, "simulation.json")
    simulations = convert.json_file_to_dict(fpath)

    # Insert into db.
    for simulation in simulations:
        # ... ensure CV cross references are lower-case.
        for key in simulation['associations'].keys():
            simulation['associations'][key] = simulation['associations'][key].lower()

        # ... hydrate new simulation;
        sim = db_types.Simulation()
        sim.activity = simulation['associations']['activity']
        sim.compute_node = simulation['associations']['compute_node']
        sim.compute_node_login = simulation['associations']['compute_node_login']
        sim.compute_node_machine = simulation['associations']['compute_node_machine']
        sim.ensemble_member = simulation['ensemble_member']
        sim.execution_end_date = simulation['execution_end_date']
        sim.execution_start_date = simulation['execution_start_date']
        sim.experiment = simulation['associations']['experiment']
        sim.model = simulation['associations']['model']
        sim.name = simulation['name']
        sim.output_end_date = simulation['output_end_date']
        sim.output_start_date = simulation['output_start_date']
        sim.space = simulation['associations']['space']
        try:
            sim.parent_simulation_name = simulation['parent_simulation']
        except KeyError:
            pass
        try:
            sim.parent_simulation_branch_date = simulation['parent_simulation_branch_date']
        except KeyError:
            pass

        # Set hash id.
        sim.hashid = sim.get_hashid()

        # ... insert into db.
        db_session.add(sim)


def execute():
    """Sets up a database.

    """
    db_session.assert_is_live()

    # Initialize schemas.
    db_session.sa_engine.execute(DropSchema('public'))
    for schema in db_types.SCHEMAS:
        db_session.sa_engine.execute(CreateSchema(schema))

    # Initialize tables.
    db_types.metadata.create_all(db_session.sa_engine)

    # Seed tables.
    _init_cv_terms()
    _init_simulations()
