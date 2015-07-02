# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao.py
   :platform: Unix
   :synopsis: Set of core data access operations.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import random

from prodiguer.db.pgres import session
from prodiguer.db.pgres.type_utils import assert_type



def sort(etype, collection):
    """Sorts collection via type sort key.

    :param etype: A supported entity type.
    :type etype: class

    :param collection: Collection of entities.
    :type collection: list

    :returns: Sorted collection.
    :rtype: list

    """
    assert_type(etype)

    return [] if collection is None else etype.get_sorted(collection)


def get_all(etype):
    """Gets all instances of the entity.

    :param etype: A supported entity type.
    :type etype: class

    :returns: Entity collection.
    :rtype: list

    """
    return get_by_facet(etype, order_by=etype.id, get_iterable=True)


def get_by_facet(etype, qfilter=None, order_by=None, get_iterable=False):
    """Gets entity instance by facet.

    :param class etype: A supported entity type.
    :param expression qfilter: Query filter expression.
    :param expression order_by: Sort expression.
    :param bool get_iterable: Flag indicating whether to return an iterable or not.

    :returns: Entity or entity collection.
    :rtype: Sub-class of types.Entity

    """
    assert_type(etype)

    qry = session.query(etype)
    if qfilter is not None:
        qry = qry.filter(qfilter)
    if order_by is not None:
        qry = qry.order_by(order_by)

    return exec_query(etype, qry, get_iterable)


def exec_query(etype, qry, get_iterable=False):
    """Executes a query and return result (sorted if is a collection).

    :param class etype: A supported entity type.
    :param expression qry: Query expressions.
    :param bool get_iterable: Flag indicating whether to return an iterable or not.

    :returns: Entity or entity collection.
    :rtype: Sub-class of types.Entity
    """
    return sort(etype, qry.all()) if get_iterable else qry.first()


def get_random(etype):
    """Returns a random instance.

    :param etype: Type of instance to be returned.
    :type etype: class

    :returns: A random item from the cache.
    :rtype: Sub-class of types.Entity

    """
    all = get_all(etype)

    return None if not len(all) else all[random.randint(0, len(all) - 1)]


def get_random_sample(etype):
    """Returns a random instance sample.

    :param etype: Type of instances to be returned.
    :type etype: class

    :returns: A random sample from the db.
    :rtype: list

    """
    all = get_all(etype)

    return [] if not len(all) else random.sample(all, random.randint(1, len(all)))


def get_by_id(etype, id):
    """Gets entity instance by id.

    :param etype: A supported entity type.
    :type etype: class

    :param id: id of entity.
    :type id: int

    :returns: Entity with matching id.
    :rtype: Sub-class of types.Entity

    """
    return get_by_facet(etype, qfilter=etype.id==id)


def get_by_name(etype, name):
    """Gets an entity instance by it's name.

    :param etype: A supported entity type.
    :type etype: class

    :param name: Name of entity.
    :type name: str

    :returns: Entity with matching name.
    :rtype: Sub-class of types.Entity

    """
    return get_by_facet(etype, qfilter=etype.name==name)


def get_count(etype, qfilter=None):
    """Gets count of entity instances.

    :param etype: A supported entity type.
    :type etype: class

    :returns: Entity collection count.
    :rtype: int

    """
    assert_type(etype)

    q = session.query(etype)
    if qfilter is not None:
        q = q.filter(qfilter)

    return q.count()


def insert(entity):
    """Adds a newly created model to the session.

    :param item: A supported entity instance.
    :type item: Sub-class of types.Entity

    """
    session.add(entity)

    return entity


def delete(entity):
    """Marks entity instance for deletion.

    :param item: A supported entity instance.
    :type item: Sub-class of types.Entity

    """
    session.delete(entity)


def delete_all(etype):
    """Deletes all entities of passed type.

    :param etype: A supported entity type.
    :type etype: class

    """
    assert_type(etype)

    q = session.query(etype)
    q.delete()


def delete_by_facet(etype, expression):
    """Delete entity instance by id.

    :param etype: A supported entity type.
    :type etype: class

    :param facet: Entity facet.
    :type facet: expression

    :param facet: Entity facet value.
    :type facet: object

    """
    assert_type(etype)

    q = session.query(etype)
    q = q.filter(expression)
    q.delete()


def delete_by_id(etype, id):
    """Delete entity instance by id.

    :param etype: A supported entity type.
    :type etype: class

    :param id: id of entity.
    :type id: int

    """
    delete_by_facet(etype, etype.id==id)


def delete_by_name(etype, name):
    """Deletes an entity instance by it's name.

    :param etype: A supported entity type.
    :type etype: class

    :param name: Name of entity.
    :type name: str

    """
    delete_by_facet(etype, etype.name==name)
