# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.session.py
   :platform: Unix
   :synopsis: Database session manager.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import logging

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine

from prodiguer.utils import config



# SQLAlchemy engine.
sa_engine = None

# SQLAlchemy session.
sa_session = None


# Set sqlalchemy logging.
loggers = [
    ('sqlalchemy.dialects', logging.NOTSET),
    ('sqlalchemy.engine', logging.NOTSET),
    ('sqlalchemy.orm', logging.NOTSET),
    ('sqlalchemy.pool', logging.NOTSET)
]
logging.basicConfig()
for logger, level in loggers:
    logging.getLogger(logger).setLevel(level)


def assert_is_live():
    """Ensures that session has been established.

    """
    msg = "ERROR :: You have not initialised the db session"
    assert sa_session is not None, msg
    assert sa_engine is not None, msg


def start(connection=None):
    """Starts a db session.

    :param connection: Either a db connection string or a SQLAlchemy db engine.
    :type connection: str | sqlalchemy.Engine

    """
    global sa_engine
    global sa_session

    # Implicit end.
    end()

    # Set connection if necessary.
    if connection is None:
        connection = config.db.pgres.main

    # Set engine.
    if isinstance(connection, Engine):
        sa_engine = connection
    else:
        sa_engine = create_engine(unicode(connection), echo=False)

    # Set session.
    sa_session = sessionmaker(bind=sa_engine)()


def end():
    """Ends a session.

    """
    global sa_engine
    global sa_session

    if sa_engine is not None:
        sa_engine = None
    if sa_session is not None:
        sa_session.close()
        sa_session = None


def commit():
    """Commits a session.

    """
    if sa_session is not None:
        sa_session.commit()


def rollback():
    """Rolls back a session.

    """
    if sa_session is not None:
        sa_session.rollback()


def insert(instance, auto_commit=True):
    """Adds a newly created type instance to the session and optionally commits the session.

    :param Entity instance: A db type instance.
    :param bool auto_commit: Flag indicating whether a commit is to be issued.

    """
    if instance is not None and sa_session is not None:
        sa_session.add(instance)
        if auto_commit:
            commit()

    return instance


def add(instance, auto_commit=True):
    """Adds a newly created type instance to the session and optionally commits the session.

    :param Entity instance: A db type instance.
    :param bool auto_commit: Flag indicating whether a commit is to be issued.

    """
    return insert(instance, auto_commit)


def delete(instance, auto_commit=True):
    """Marks a type instance for deletion and optionally commits the session.

    :param instance: A db type instance.
    :type instance: sub-class of Entity

    :param auto_commit: Flag indicating whether a commit is to be issued.
    :type auto_commit: bool

    """
    if instance is not None and sa_session is not None:
        sa_session.delete(instance)
        if auto_commit:
            commit()


def update(instance, auto_commit=True):
    """Marks a type instance for update and optionally commits the session.

    :param db.Entity instance: A db type instance.
    :param bool auto_commit: Flag indicating whether a commit is to be issued.

    """
    if instance is not None and sa_session is not None:
        if auto_commit:
            commit()

    return instance


def query(*etypes):
    """Begins a query operation against a session.

    """
    if len(etypes) == 0 or sa_session is None:
        return None

    q = None
    for etype in etypes:
        q = sa_session.query(etype) if q is None else q.join(etype)
    return q
