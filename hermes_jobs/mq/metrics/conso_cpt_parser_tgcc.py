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

# CMIP6 project code.
_CMIP6 = 'cmip6'

# Primary CMIP6 accounting project code @ TGCC.
_GENCMIP6 = 'gencmip6'


def yield_blocks(cpt):
    """Yields conso blocks from a TGCC cpt file.

    """
    for start, end in _get_indexes(cpt):
        for consumption in _yield_consumptions(cpt, start, end):
            yield _get_block(cpt, consumption, start, end)


def _get_block(cpt, consumption, start, end):
    """Returns CPT block.

    """
    project, sub_project = _get_block_project_info(cpt, consumption, start)

    return {
        'machine': cpt[start].split()[5].lower(),
        'node': cpt[start].split()[6].lower(),
        'consumption_date': dt.datetime.strptime(
            "{} 23:59:59".format(cpt[start].split()[-1]), "%Y-%m-%d %H:%M:%S"),
        'consumption': [(l[0], float(l[-1])) for l in consumption],
        'project': project,
        'project_allocation': float(cpt[end - 3].split()[-1]),
        'project_end_date': dt.datetime.strptime(cpt[end].split()[-1], "%Y-%m-%d"),
        'project_start_date': dt.datetime(_YEAR, 01, 01),
        'project_total': float(cpt[end - 4].split()[-1]),
        'sub_project': sub_project,
        'sub_total': sum([float(l[-1]) for l in consumption], float())
    }


def _get_indexes(cpt):
    """Returns CPT block start/end positions.

    """
    return zip(
        [i for i, v in enumerate(cpt) if v.startswith('accounting')],
        [i for i, v in enumerate(cpt) if v.startswith('project')]
    )


def _yield_consumptions(cpt, start, end):
    """Yields consumption lines considered to be a block.

    """
    # Get consumption lines.
    lines = [l.split() for l in cpt[start + 2: end - 4]]

    # Exclude interim column headers.
    lines = [l for l in lines if not l[0].startswith("login")]

    # Set sub-totals.
    subtotals = []
    for idx, line in enumerate(lines):
        if line[0].startswith('subtotal'):
            subtotals.append(idx)

    # If no subtotals then simply return as is.
    if not subtotals:
        yield lines

    # If only one subtotal then return all lines except last.
    if len(subtotals) == 1:
        yield lines[0:-1]

    # Otherwise return blocks grouped by sub-project.
    for idx, subtotal in enumerate(subtotals):
        if idx == 0:
            yield lines[0:subtotal]
        else:
            yield lines[subtotals[idx - 1] + 1:subtotal]


def _get_block_project_info(cpt, consumption, start):
    """Returns block project / sub-project.

    """
    # Derive project from CPT block header.
    project = cpt[start].split()[3].lower()

    # Derive sub-project:
    # ... from first consumption line;
    if len(consumption[0]) == 3:
        return project, consumption[0][1]
    # ... cmip6 related but not gencimp6;
    elif project.endswith(_CMIP6) and project != _GENCMIP6:
        return _GENCMIP6, project
    # ... otherwise sub-project is None.
    else:
        return project, None
