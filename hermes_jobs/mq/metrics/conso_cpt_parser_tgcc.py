# -*- coding: utf-8 -*-

"""
.. module:: conso_utils.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Conso related utility functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import collections
import datetime as dt



# Year constant.
_YEAR = dt.datetime.utcnow().year


def yield_blocks(cpt):
    """Yields conso blocks from a TGCC cpt file.

    """
    for start, end in _get_indexes(cpt):
        consumption = _get_consumption(cpt, start, end)
        yield {
            'machine': cpt[start].split()[5].lower(),
            'node': cpt[start].split()[6].lower(),
            'consumption_date': dt.datetime.strptime(
                "{} 23:59:59".format(cpt[start].split()[-1]), "%Y-%m-%d %H:%M:%S"),
            'consumption': consumption,
            'project': cpt[start].split()[3].lower(),
            'project_allocation': float(cpt[end - 3].split()[-1]),
            'project_end_date': dt.datetime.strptime(cpt[end].split()[-1], "%Y-%m-%d"),
            'project_start_date': dt.datetime(_YEAR, 01, 01),
            'project_total': float(cpt[end - 4].split()[-1]),
            'subtotals': _get_subtotals(consumption)
        }


def _get_indexes(cpt):
    """Returns CPT block start/end positions.

    """
    return zip(
        [i for i, v in enumerate(cpt) if v.startswith('accounting')],
        [i for i, v in enumerate(cpt) if v.startswith('project')]
    )


def _get_consumption(cpt, start, end):
    """Yields consumption lines considered to be a block.

    """
    # Get consumption lines.
    lines = [l.split() for l in cpt[start + 2: end - 4]]

    # Exclude interim column headers.
    lines = [l for l in lines if not l[0].startswith("login")]

    # Exclude sub-totals.
    lines = [l for l in lines if not l[0].startswith("subtotal")]

    # Convert login totals to floats.
    for line in lines:
        line[-1] = float(line[-1])

    # Inject null sub-project (if appropriate).
    lines = [l if len(l) == 3 else (l[0], None, l[1]) for l in lines]

    return lines


def _get_subtotals(consumption):
    """Returns consumption by sub-project subtotals.

    """
    subtotals = collections.defaultdict(float)
    for _, sub_project, total_hours in consumption:
        if sub_project is not None:
            subtotals[sub_project] += total_hours

    return [(k, v) for k, v in subtotals.iteritems()]
