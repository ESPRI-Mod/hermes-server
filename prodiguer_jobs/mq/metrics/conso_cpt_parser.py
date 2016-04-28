# -*- coding: utf-8 -*-

"""
.. module:: conso_utils.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Conso related utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime as dt
import os



# Supported HPC's.
_HPC_TGCC = 'tgcc'
_HPC_IDRIS = 'idris'

# Year constant.
_YEAR = dt.datetime.utcnow().year


def get_blocks(centre, cpt, project=None):
    """Returns conso blocks from contents of a cpt file.

    :param str centre: HPC code, e.g. tgcc.
    :param str cpt: Either a cpt file path or cpt contents.
    :param str project: Accounting project, e.g. gencmip6.

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
        return _get_blocks_tgcc(cpt)
    elif centre == _HPC_IDRIS:
        return _get_blocks_idris(project, cpt)
    else:
        raise KeyError("HPC not supported: {}".format(centre))


def _get_blocks_idris(project, cpt):
    """Returns conso blocks from an IDRIS cpt file.

    """
    # Set metric block start end positions.
    indexes = zip(
        [i for i, v in enumerate(cpt) if v.find('mise a jour') > -1],
        [i for i, v in enumerate(cpt) if v.find('totaux') > -1 and len(v.split()) == 4]
        )

    # Set metric blocks.
    blocks = []
    for start, end in indexes:
        blocks.append({
            'project': project,
            'sub_project': None,
            'machine': cpt[start + 1].split()[-1][:-1],
            'node': 'standard',
            'consumption_date': dt.datetime.strptime(
                "{} {}".format(cpt[start].split()[-2:][0],
                               cpt[start].split()[-2:][1]),
                               "%d/%m/%Y %H:%M"
            ),
            'consumption': [(l[-5], float() if l[-3] == '-' else float(l[-3]))
                            for l in [l.split() for l in cpt[start + 7: end - 1]]],
            'total': float(cpt[end].split()[1]),
            'project_allocation': float(cpt[start + 3].split()[-1]),
            'project_end_date': dt.datetime(_YEAR, 12, 31, 23, 59, 59),
            'project_start_date': dt.datetime(_YEAR, 01, 01)
            })

    return blocks


def _get_blocks_tgcc(cpt):
    """Returns conso blocks from a TGCC cpt file.

    """
    # Set metric block start end positions.
    indexes = zip(
        [i for i, v in enumerate(cpt) if v.startswith('accounting')],
        [i for i, v in enumerate(cpt) if v.startswith('project')]
        )

    # Set metric blocks.
    blocks = []
    for start, end in indexes:
        blocks.append({
            'allocation': None,
            'project': cpt[start].split()[3].lower(),
            'sub_project': None,
            'machine': cpt[start].split()[5].lower(),
            'node': cpt[start].split()[6].lower(),
            'consumption_date': dt.datetime.strptime(
                "{} 23:59:59".format(cpt[start].split()[-1]), "%Y-%m-%d %H:%M:%S"),
            'consumption': [(n, float(t)) for n, t in
                            [l.split() for l in cpt[start + 2: end - 4]]],
            'total': float(cpt[end - 4].split()[-1]),
            'project_allocation': float(cpt[end - 3].split()[-1]),
            'project_end_date': dt.datetime.strptime(cpt[end].split()[-1], "%Y-%m-%d"),
            'project_start_date': dt.datetime(_YEAR, 01, 01)
            })

    return blocks
