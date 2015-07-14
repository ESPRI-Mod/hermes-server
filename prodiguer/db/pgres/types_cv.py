# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_cv.py
   :platform: Unix
   :synopsis: Prodiguer controlled vocabulary db schema types.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import Column
from sqlalchemy import Unicode
from sqlalchemy import UniqueConstraint

from prodiguer.db.pgres.entity import Entity



# Database schema.
SCHEMA = 'cv'


class ControlledVocabularyTerm(Entity):
    """Represents a CV term.

    """
    # SQLAlchemy directives.
    __tablename__ = 'tbl_cv_term'
    __table_args__ = (
        UniqueConstraint('typeof' ,'name'),
        {'schema':SCHEMA}
    )

    # Attributes.
    typeof = Column(Unicode(127), nullable=False)
    name = Column(Unicode(127), nullable=False)
    display_name = Column(Unicode(127))
    synonyms = Column(Unicode(1023))

