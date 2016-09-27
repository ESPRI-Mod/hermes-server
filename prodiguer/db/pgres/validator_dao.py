# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.dao.validation.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: General data access operations validation.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.utils.validation import validate_bool
from prodiguer.utils.validation import validate_entity_type
from prodiguer.utils.validation import validate_iterable
from prodiguer.utils.validation import validate_int
from prodiguer.utils.validation import validate_ucode



def _validate_entity_id(entity_id):
    """Validates an entity identifier.

    """
    validate_int(entity_id, 'entity_id')
    if entity_id <= 0:
      raise ValueError("Entity identifier must be a positive integer")


def _validate_query_expression(expression=None):
    """Validates a query expression.

    """
    pass


def validate_delete(entity):
    """Function input validator: delete.

    """

    validate_entity_type(type(entity))
    validate_bool(entity.is_new, 'entity')
    if entity.is_new:
        raise ValueError("Cannot delete new entities.")


def validate_delete_all(etype):
    """Function input validator: delete_all.

    """
    validate_entity_type(etype)


def validate_delete_by_facet(etype, filter_expression=None):
    """Function input validator: delete_by_facet.

    """
    validate_entity_type(etype)


def validate_delete_by_id(etype, entity_id):
    """Function input validator: delete_by_id.

    """
    validate_entity_type(etype)
    _validate_entity_id(entity_id)


def validate_delete_by_name(etype, entity_name):
    """Function input validator: delete_by_name.

    """
    validate_entity_type(etype)
    validate_ucode(entity_name, 'entity_name')


def validate_exec_query(etype, qry, get_iterable=False):
    """Function input validator: exec_query.

    """
    validate_entity_type(etype)
    _validate_query_expression(qry)
    validate_bool(get_iterable, 'get_iterable')


def validate_get_all(etype):
    """Function input validator: get_all.

    """
    validate_entity_type(etype)


def validate_get_by_facet(etype, qfilter=None, order_by=None, get_iterable=False):
    """Function input validator: get_all.

    """
    validate_entity_type(etype)
    _validate_query_expression(qfilter)
    validate_bool(get_iterable, 'get_iterable')


def validate_get_by_id(etype, entity_id):
    """Function input validator: get_by_id.

    """
    validate_entity_type(etype)
    _validate_entity_id(entity_id)


def validate_get_by_name(etype, entity_name):
    """Function input validator: get_by_name.

    """
    validate_entity_type(etype)
    validate_ucode(entity_name, 'entity_name')


def validate_get_count(etype, qfilter=None):
    """Function input validator: get_count.

    """
    validate_entity_type(etype)
    _validate_query_expression(qfilter)


def validate_get_random(etype):
    """Function input validator: get_random.

    """
    validate_entity_type(etype)


def validate_get_random_sample(etype):
    """Function input validator: get_random_sample.

    """
    validate_entity_type(etype)


def validate_sort(etype, collection, sort_key=None):
    """Function input validator: sort.

    """
    validate_entity_type(etype)
    validate_iterable(collection, 'collection')
    if sort_key is not None:
        pass
