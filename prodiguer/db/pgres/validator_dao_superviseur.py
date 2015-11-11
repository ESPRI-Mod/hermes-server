# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.dao_superviseur_validator.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Superviseur data access operations validator.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.utils import validation



def validate_create_supervision(simulation_uid, job_uid, trigger_code):
    """Function input validator: create_supervision.

    """
    def _validate_trigger_code():
        """Validates trigger_code input parameter.

        """
        validation.validate_unicode(trigger_code, "trigger_code")

    validation.validate_uid(job_uid, "Job uid")
    validation.validate_uid(simulation_uid, "Simulation uid")
    _validate_trigger_code()
