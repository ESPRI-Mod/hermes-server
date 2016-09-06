# -*- coding: utf-8 -*-

"""
.. module:: conso_utils.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Conso related utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os

from hermes_jobs.mq.metrics import conso_cpt_parser_idris as parser_idris
from hermes_jobs.mq.metrics import conso_cpt_parser_tgcc as parser_tgcc



# Supported HPC's.
_HPC_TGCC = 'tgcc'
_HPC_IDRIS = 'idris'


def get_blocks(centre, cpt, project=None):
    """Returns conso blocks extracted from a CPT.

    :param str centre: HPC node, e.g. tgcc.
    :param str cpt: Either a CPT file path or CPT contents.
    :param str project: Accounting project (only applies to IDRIS CPT).

    """
    # Open file (if necessary).
    if os.path.isfile(cpt):
        with open(cpt, 'r') as fstream:
            cpt = fstream.read()

    # Strip empty lines & convert all text to lower case.
    cpt = [l.strip().lower() for l in cpt.split('\n') if l.strip()]

    # Route to relevant parser.
    centre = centre.lower()
    if centre == _HPC_TGCC:
        return list(parser_tgcc.yield_blocks(cpt))
    elif centre == _HPC_IDRIS:
        return list(parser_idris.yield_blocks(cpt, project))
    else:
        raise KeyError("HPC not supported: {}".format(centre))
