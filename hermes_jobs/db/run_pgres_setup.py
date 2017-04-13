# -*- coding: utf-8 -*-

"""
.. module:: run_db_pgres_setup.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Sets up postgres database.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import sqlalchemy

from hermes.db import pgres as db
from hermes.utils import config
from hermes.utils import logger



def _main():
    """Main entry point.

    """
    def setup(connection):
        """Sets up the database.

        """
        logger.log_db("Seeding begins : db = {0}".format(connection))

        # Setup db.
        with db.session.create(connection):
            db.setup.execute()

        logger.log_db("Seeding ends : db = {0}".format(connection))

    # Setup target db.
    try:
        setup(config.db.pgres.main.replace(db.constants.HERMES_DB_USER,
                                           db.constants.HERMES_DB_ADMIN_USER))
    except sqlalchemy.exc.ProgrammingError as err:
        print err
        logger.log_db_error("SETUP ERROR : are db connections still open ?")



if __name__ == '__main__':
    _main()
