# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_mq.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres import dao
from prodiguer.db.pgres import validator_dao_cv as validator
from prodiguer.db.pgres import session
from prodiguer.db.pgres import types
from prodiguer.utils import decorators
from prodiguer.utils import logger



@decorators.validate(validator.validate_create_term)
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


@decorators.validate(validator.validate_retrieve_term)
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
