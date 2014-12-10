# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.session.py
   :platform: Unix
   :synopsis: Database session manager.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os
from os.path import dirname, abspath

from sqlalchemy.schema import CreateSchema

from prodiguer.db import session, types
from prodiguer.utils import convert, rt



def _init_cv_terms():
    """Initialises set of controlled vocabulary terms.

    """
    def _extend_term(cv_type, term):
        """Injects extra information into a CV term.

        """
        term['cv_type'] = cv_type
        if 'sort_key' not in term:
            term['sort_key'] = "{0}.{1}".format(term['cv_type'], term['name'])
        term['sort_key'] = term['sort_key'].lower()

    from prodiguer import cv

    # Iterate cv types.
    cv.cache.load()
    for cv_type in cv.cache.get_types():
        rt.log_db("SEEDING CV TERMS :: {0}".format(cv_type))

        # Load & sort collection.
        terms = cv.cache.get_collection(cv_type)
        for term in terms:
            _extend_term(cv_type, term)
        terms = sorted(terms, key=lambda k: k['sort_key'])

        # Insert into db.
        for term in terms:
            term = types.Convertor.from_dict(types.CvTerm, term)
            session.add(term)


def _init_simulations():
    """Initialises set of simulations.

    """
    # Set simulations from simulation.json file.
    fpath = "/".join(dirname(abspath(__file__)).split("/")[0:-1])
    fpath = os.path.join(fpath, "cv")
    fpath = os.path.join(fpath, "json")
    fpath = os.path.join(fpath, "simulation.json")
    simulations = convert.json_file_to_namedtuple(fpath)

    # Insert into db.
    for simulation in simulations:
        sim = types.Simulation()
        sim.activity = simulation.associations.activity
        sim.compute_node = simulation.associations.compute_node
        sim.compute_node_login = simulation.associations.compute_node_login
        sim.compute_node_machine = simulation.associations.compute_node_machine
        sim.ensemble_member = simulation.ensemble_member
        sim.execution_end_date = simulation.execution_end_date
        sim.execution_state = simulation.associations.execution_state
        sim.execution_start_date = simulation.execution_start_date
        sim.experiment = simulation.associations.experiment
        sim.model = simulation.associations.model
        sim.name = simulation.name
        sim.output_end_date = simulation.output_end_date
        sim.output_start_date = simulation.output_start_date
        sim.space = simulation.associations.space
        try:
            sim.parent_simulation_name = simulation.parent_simulation
        except AttributeError:
            pass
        try:
            sim.parent_simulation_branch_date = simulation.parent_simulation_branch_date
        except AttributeError:
            pass

        session.add(sim)


def _setup_cv():
    """Sets up cv tables."""
    _init_cv_terms()
    _init_simulations()


def execute():
    """Sets up a database.

    """
    session.assert_is_live()

    # Create schemas.
    for schema in types.SCHEMAS:
        session.sa_engine.execute(CreateSchema(schema))

    # Create tables.
    types.metadata.create_all(session.sa_engine)

    # Set up cv tables.
    _setup_cv()
