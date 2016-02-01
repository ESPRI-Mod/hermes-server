# -*- coding: utf-8 -*-

"""
.. module:: run_pgres_reset_cv_table.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Resets prodiguer postgres database tables after manual cv updates.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db import pgres as db
from prodiguer.utils import logger



def _main():
    """Main entry point.

    """
    logger.log_db("Reset cv table begins")

    # Reinitialise terms from cv files.
    with db.session.create():
        db.setup.init_cv_terms()

    logger.log_db("Reset cv table complete")


if __name__ == '__main__':
    _main()
