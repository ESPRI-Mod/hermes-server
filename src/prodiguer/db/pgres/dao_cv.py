# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_mq.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.cv import validation as cv_validator
from prodiguer.db.pgres import dao
from prodiguer.db.pgres import session
from prodiguer.db.pgres import types
from prodiguer.db.pgres.setup import init_cv_terms
from prodiguer.utils import decorators
from prodiguer.utils import logger



def _validate_create_term(
    term_type,
    term_name,
    term_display_name,
    ):
    """Validates create term inputs.

    """
    cv_validator.validate_term_type(term_type)
    cv_validator.validate_term_name(term_type, term_name)
    cv_validator.validate_term_display_name(term_display_name)


@decorators.validate(_validate_create_term)
def create_term(
    term_type,
    term_name,
    term_display_name,
    ):
    """Creates a CV term in db.

    :param str term_type: Type of term being created.
    :param str term_name: Name of term being created.
    :param str term_display_name: Display name of term being created.

    :returns: Newly created CV term.
    :rtype: types.ControlledVocabularyTerm

    """
    instance = types.ControlledVocabularyTerm()
    instance.typeof = term_type
    instance.name = term_name
    instance.display_name = term_display_name

    # Push to db.
    session.add(instance)

    return instance


def retrieve_term(term_type, term_name):
    """Returns a CV term from db.

    :param str term_type: Type of term being retrieved.
    :param str term_name: Name of term being retrieved.

    :returns: A CV term.
    :rtype: types.ControlledVocabularyTerm

    """
    qry = session.query(types.ControlledVocabularyTerm)
    qry = qry.filter(types.ControlledVocabularyTerm.typeof == unicode(term_type))
    qry = qry.filter(types.ControlledVocabularyTerm.name == unicode(term_name))

    return qry.first()


def reset_terms():
    logger.log_db("Deleting existing cv.tbl_cv_term records")
    dao.delete_all(types.ControlledVocabularyTerm)
    init_cv_terms()
