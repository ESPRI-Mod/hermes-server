# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.session.py
   :platform: Unix
   :synopsis: Database session manager.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""

# Module imports.
from sqlalchemy.schema import CreateSchema

from . import session, types
from .. utils import runtime as rt


def _setup_cv():
    """Sets up cv tables."""
    from .. import cv

    rt.log_db("SEEDING BEGINS")

    # Iterate cv types.
    for etype in types.CV:
        rt.log_db("SEEDING TABLE :: {0}.{1}".format(etype.__table__.schema, etype.__table__.name))

        # Iterate cv terms.
        cv_terms = sorted(cv.read(etype), key=lambda k: k['id']) 
        for cv_term in cv_terms:
            # Create & persist.
            session.add(types.Convertor.from_dict(etype, cv_term))

    rt.log_db("SEEDING ENDS")


def execute():
    """Sets up a database.

    """
    session.assert_is_live()

    # Create schemas.
    for s in types.SCHEMAS:
        session.sa_engine.execute(CreateSchema(s))    

    # Create tables.
    types.metadata.create_all(session.sa_engine)

    # Set up cv tables.
    _setup_cv()
