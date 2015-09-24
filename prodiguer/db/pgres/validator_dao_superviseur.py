# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_superviseur_validator.py
   :copyright: Copyright "Apr 26, 2013", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Superviseur data access operations validator.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.db.pgres import validator



def validate_create_supervision(simulation_uid, job_uid, trigger_code):
    """Function input validator: create_supervision.

    """
    def _validate_trigger_code():
        """Validates trigger_code input parameter.

        """
        validator.validate_unicode(trigger_code, "trigger_code")

    validator.validate_job_uid(job_uid)
    validator.validate_simulation_uid(simulation_uid)
    _validate_trigger_code()
