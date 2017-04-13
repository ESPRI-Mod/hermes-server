# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.dao_mq.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: MQ data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from hermes.db.pgres import validator_dao_cv as validator
from hermes.db.pgres import session
from hermes.db.pgres import types
from hermes.utils import decorators



@decorators.validate(validator.validate_create_term)
def create_term(
    term_type,
    term_name,
    term_display_name,
    term_uid
    ):
    """Creates a CV term in db.

    :param str term_type: Type of term being created.
    :param str term_name: Name of term being created.
    :param str term_display_name: Display name of term being created.
    :param str term_uid: UID of term being created.

    :returns: Newly created CV term.
    :rtype: types.ControlledVocabularyTerm

    """
    instance = types.ControlledVocabularyTerm()
    instance.typeof = term_type
    instance.name = term_name
    instance.display_name = term_display_name
    instance.uid = term_uid

    return session.insert(instance)


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
