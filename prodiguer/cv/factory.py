# -*- coding: utf-8 -*-

"""
.. module:: cv.factory.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary factory for creating new cv terms.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import uuid

import arrow

from prodiguer.cv import constants
from prodiguer.cv.parser import parse_term_data
from prodiguer.cv.parser import parse_term_display_name
from prodiguer.cv.parser import parse_term_name
from prodiguer.cv.parser import parse_term_type



def create(term_type, term_name, term_data=None):
    """Creates a cv term.

    :param str term_type: Type of CV term being parsed, e.g. accounting project.
    :param str term_name: Name of CV term being parsed, e.g. gencmip6.
    :param dict term_data: User defined term data.

    :returns: Created cv term.
    :rtype: dict

    """
    term = {
        "meta": {
            "associations": [],
            "create_date": unicode(arrow.utcnow()),
            "display_name": parse_term_display_name(term_type, term_name, False),
            "domain": "climate",
            "name": parse_term_name(term_type, term_name, False),
            "status": constants.TERM_GOVERNANCE_STATE_NEW,
            "type": parse_term_type(term_type),
            "uid": unicode(uuid.uuid4())
        }
    }

    # Update with user defined term data.
    term.update(parse_term_data(term_data))

    return term
