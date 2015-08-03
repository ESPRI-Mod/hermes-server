# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao.validator.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: General data access operations validator.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres import types
from prodiguer.db.pgres import validator



def _validate_entity_type(etype):
    """Validates a type to ensure that it is a recognized entity.

    """
    if etype not in types.SUPPORTED:
        raise TypeError('DB entity type is unknown :: {0}'.format(etype))


def _validate_entity_id(entity_id):
    """Validates an entity identifier.

    """
    validator.validate_int(entity_id, 'entity_id')
    if entity_id <= 0:
      raise ValueError("Entity identifier must be a positive integer")


def _validate_query_expression(qry=None):
    """Validates a query expression.

    """
    pass


def validate_delete(entity):
    """Function input validator: delete.

    """
    _validate_entity_type(type(entity))
    validator.validate_bool(entity.is_new, 'entity')
    if entity.is_new:
        raise ValueError("Cannot delete new entities.")


def validate_delete_all(etype):
    """Function input validator: delete_all.

    """
    _validate_entity_type(etype)


def validate_delete_by_facet(etype, filter_expression):
    """Function input validator: delete_by_facet.

    """
    _validate_entity_type(etype)


def validate_delete_by_id(etype, entity_id):
    """Function input validator: delete_by_id.

    """
    _validate_entity_type(etype)
    _validate_entity_id(entity_id)


def validate_delete_by_name(etype, entity_name):
    """Function input validator: delete_by_name.

    """
    _validate_entity_type(etype)
    validator.validate_unicode(entity_name, 'entity_name')


def validate_exec_query(etype, qry, get_iterable=False):
    """Function input validator: exec_query.

    """
    _validate_entity_type(etype)
    _validate_query_expression(qry)
    validator.validate_bool(get_iterable, 'get_iterable')


def validate_get_all(etype):
    """Function input validator: get_all.

    """
    _validate_entity_type(etype)


def validate_get_by_facet(etype, qfilter=None, order_by=None, get_iterable=False):
    """Function input validator: get_all.

    """
    _validate_entity_type(etype)
    _validate_query_expression(qfilter)
    validator.validate_bool(get_iterable, 'get_iterable')


def validate_get_by_id(etype, entity_id):
    """Function input validator: get_by_id.

    """
    _validate_entity_type(etype)
    _validate_entity_id(entity_id)


def validate_get_by_name(etype, entity_name):
    """Function input validator: get_by_name.

    """
    _validate_entity_type(etype)
    validator.validate_unicode(entity_name, 'entity_name')


def validate_get_count(etype, qfilter=None):
    """Function input validator: get_count.

    """
    _validate_entity_type(etype)
    _validate_query_expression(qfilter)


def validate_get_random(etype):
    """Function input validator: get_random.

    """
    _validate_entity_type(etype)


def validate_get_random_sample(etype):
    """Function input validator: get_random_sample.

    """
    _validate_entity_type(etype)


def validate_insert(entity):
    """Function input validator: insert.

    """
    _validate_entity_type(type(entity))
    validator.validate_bool(entity.is_new, 'entity')
    if not entity.is_new:
        raise ValueError("Cannot reinsert existing entities.")


def validate_sort(etype, collection):
    """Function input validator: sort.

    """
    _validate_entity_type(etype)
    validator.validate_iterable(collection, 'collection')
