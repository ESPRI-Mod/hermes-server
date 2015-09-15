# -*- coding: utf-8 -*-

"""
.. module:: run_pgres_reset_cv_table.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Resets prodiguer postgres database tables after manual cv updates.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db import pgres as db
from prodiguer.utils import config
from prodiguer.utils import logger



def _main():
    """Main entry point.

    """
    logger.log_db("Reset cv table begins")

    # Start session.
    db.session.start(config.db.pgres.main)

    # Delete existing terms.
    db.dao.delete_all(db.types.ControlledVocabularyTerm)

    # Reinitialise terms from cv files.
    db.setup.init_cv_terms()

    # End session.
    db.session.end()

    logger.log_db("Reset cv table complete")


if __name__ == '__main__':
    _main()
