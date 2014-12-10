# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.types_shared.py
   :copyright: Copyright "May 21, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Set of db types shared across other schemas.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import (
    Boolean,
    Column,
    Enum,
    Unicode
    )

from prodiguer import cv
from prodiguer.db.type_utils import Entity



# CV collection type enum.
ControlledVocabularyTypeEnum = \
    Enum(cv.constants.CV_TYPE_ACTIVITY,
         cv.constants.CV_TYPE_INSTITUTE,
         cv.constants.CV_TYPE_COMPUTE_NODE,
         cv.constants.CV_TYPE_COMPUTE_NODE_LOGIN,
         cv.constants.CV_TYPE_COMPUTE_NODE_MACHINE,
         cv.constants.CV_TYPE_EXPERIMENT,
         cv.constants.CV_TYPE_EXPERIMENT_GROUP,
         cv.constants.CV_TYPE_MESSAGE_TYPE,
         cv.constants.CV_TYPE_MESSAGE_APPLICATION,
         cv.constants.CV_TYPE_MESSAGE_PRODUCER,
         cv.constants.CV_TYPE_MESSAGE_USER,
         cv.constants.CV_TYPE_MODEL,
         cv.constants.CV_TYPE_MODEL_FORCING,
         cv.constants.CV_TYPE_SIMULATION_SPACE,
         cv.constants.CV_TYPE_SIMULATION_STATE,
         schema='shared',
         name='ControlledVocabularyTypeEnum')


class CvTerm(Entity):
    """A controlled vocabulary term.

    """
    # Sqlalchemy directives.
    __tablename__ = 'tbl_cvterm'
    __table_args__ = (
        {'schema':'shared'}
    )

    # Attributes.
    cv_type = Column(ControlledVocabularyTypeEnum, nullable=False)
    name = Column(Unicode(255), nullable=False)
    synonyms = Column(Unicode(2047))
    description = Column(Unicode(1023))
    url = Column(Unicode(1023))
    sort_key = Column(Unicode(511))
    is_active = Column(Boolean, nullable=False, default=True)
    is_reviewed = Column(Boolean, nullable=False, default=True)
