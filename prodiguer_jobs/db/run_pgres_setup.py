# -*- coding: utf-8 -*-

"""
.. module:: run_db_pgres_setup.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Sets up prodiguer postgres database.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import sqlalchemy

from prodiguer.db import pgres as db
from prodiguer.utils import config
from prodiguer.utils import logger



def _main():
    """Main entry point.

    """
    def setup(connection):
        """Sets up the database.

        """
        logger.log_db("Seeding begins : db = {0}".format(connection))

        # Start session.
        db.session.start(connection)

        # Setup db.
        db.setup.execute()

        # End session.
        db.session.end()

        logger.log_db("Seeding ends : db = {0}".format(connection))

    # Setup target db.
    try:
        setup(config.db.pgres.main.replace(db.constants.PRODIGUER_DB_USER,
                                           db.constants.PRODIGUER_DB_ADMIN_USER))
    except sqlalchemy.exc.ProgrammingError as err:
        print err
        logger.log_db_error("SETUP ERROR : are db connections still open ?")



if __name__ == '__main__':
    _main()
