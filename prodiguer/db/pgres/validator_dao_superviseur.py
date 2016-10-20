# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.dao_superviseur_validator.py
   :copyright: Copyright "Mar 21, 2015", IPSL
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Superviseur data access operations validator.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.utils.validation import validate_int
from prodiguer.utils.validation import validate_uid
from prodiguer.utils.validation import validate_ucode



def validate_create_supervision(simulation_uid, job_uid, trigger_code):
    """Function input validator: create_supervision.

    """
    validate_uid(job_uid, "Job uid")
    validate_uid(simulation_uid, "Simulation uid")
    validate_ucode(trigger_code, "trigger_code")


def validate_retrieve_supervision(identifer):
    """Function input validator: create_supervision.

    """
    validate_int(identifer)
