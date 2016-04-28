# -*- coding: utf-8 -*-

"""
.. module:: run_pgres_init_conso_tables.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Initializes conso related tables.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime as dt
import json

from sqlalchemy.exc import IntegrityError

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_conso as dao



def _get_initialisation_data():
    """Returns data used to initialise database.

    """
    fpath = __file__.replace(".py", ".json")
    with open(fpath, 'r') as data:
        return json.loads(data.read())


def _persist_allocation(row):
    """Persists resource allocation information to the database.

    """
    try:
        dao.persist_allocation(
            row['centre'],
            row['name'],
            None,               # sub-project
            row['machine'],
            row['node'],
            dt.datetime.strptime(row['start'], "%Y-%m-%d %H:%M:%S"),
            dt.datetime.strptime(row['end'], "%Y-%m-%d %H:%M:%S"),
            row['alloc'],
            True,
            True
            )
    except IntegrityError:
        db.session.rollback()


def _main():
    """Main entry point.

    """
    data = _get_initialisation_data()
    with db.session.create():
        for row in data['projects']:
            _persist_allocation(row)



if __name__ == '__main__':
    _main()
