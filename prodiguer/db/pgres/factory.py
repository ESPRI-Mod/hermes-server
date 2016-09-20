# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.factory.py
   :platform: Unix
   :synopsis: Database type factory used in unit testing.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import collections
import datetime
import random
import uuid

from sqlalchemy import inspect

from prodiguer.db.pgres import session
from prodiguer.db.pgres import types
from prodiguer.utils import validation



# Set of created type instances.
_created = collections.OrderedDict()


# Set of dependencies.
_dependents = { t: [] for t in types.SUPPORTED }


# Set of scalar type factories.
_scalar_factories = {
    int: lambda: random.randint(0, 9999999),
    unicode: lambda: unicode(uuid.uuid4())[:15],
    str: lambda: str(uuid.uuid4())[:15],
    bool: lambda: bool(random.randint(0, 1)),
    datetime.datetime: lambda: datetime.datetime.utcnow(),
    float: lambda: random.random(),
    long: lambda: long(random.randint(0, 9999999)),
    uuid.UUID : lambda: uuid.uuid4()
}


def _get_etype(schema, tbl):
    """Gets first type with matching schema/table name."""
    for etype in types.SUPPORTED:
        orm_mapping = inspect(etype).tables[0]
        if orm_mapping.schema == schema and orm_mapping.name == tbl:
            return etype

    return None


def _set_scalar(i, c):
    """Sets value of a scalue attribute.

    """
    stype = c.type.python_type
    if stype in _scalar_factories:
        setattr(i, c.name, _scalar_factories[stype]())
    else:
        raise TypeError("Unsupported scalar type :: {0}.".format(stype))


def _set_fk(etype, i, c, fk):
    """Sets value of a foreign key attribute.

    """
    schema, tbl, col = fk.target_fullname.split('.')
    fk_etype = _get_etype(schema, tbl)
    fki = create(fk_etype)

    if fki is not None:
        if fki not in _dependents[etype]:
            _dependents[etype].append(fki)
        setattr(i, c.name, fki.id)
    else:
        raise ValueError("Unsupported foreign key instance :: {0}.".format(fk_etype))


def create(etype, force=False, commit=False):
    """Creates and returns a type instance.

    :param class etype: Type of instance being created.
    :param bool force: Flag indicating whether the instance must be created.
    :param bool commit: Flag indicating whether the instance will be committed.

    :returns: A type instance.
    :rtype: A instance of a sub-class of db.Entity.

    """
    validation.validate_entity_type(etype)

    # Return cached if appropriate (prevents infinite recursion).
    if etype in _created:
        return _created[etype]

    # Instantiate.
    i = _created[etype] = etype()

    # Hydrate via sqlalchemy column mappings.
    for c in [c for c in inspect(etype).columns if not c.nullable]:
        if not len(c.foreign_keys) and c.name != 'id':
            _set_scalar(i, c)
        elif len(c.foreign_keys) == 1:
            _set_fk(etype, i, c, list(c.foreign_keys)[0])

    # Update session.
    session.insert(i)
    if commit:
        session.commit()

    return i


def _clean(i, etype):
    """Clean set of dependent instances.

    """
    for dependents in _dependents.values():
        if i in dependents:
            dependents.remove(i)
    del _created[etype]
    _dependents[etype] = []
    session.delete(i)


def _is_cleanable(i):
    """Determines whether an instance can be deleted.

    """
    for dependents in _dependents.values():
        if i in dependents:
            return False

    return True


def reset():
    """Resets the cache of previously created types.

    """
    # Delete instances without dependents.
    for etype, i in _created.items():
        if _is_cleanable(i):
            _clean(i, etype)

    # Loop if necessary.
    if len(_created):
        reset()
