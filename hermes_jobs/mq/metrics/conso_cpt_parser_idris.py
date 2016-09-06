# -*- coding: utf-8 -*-

"""
.. module:: conso_cpt_parser_idris.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: IDRIS HPC CPT file parser.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import datetime as dt



# Year constant.
_YEAR = dt.datetime.utcnow().year


def yield_blocks(cpt, project):
    """Yields conso blocks from a TGCC cpt file.

    """
    for start, end in _get_indexes(cpt):
        yield {
            'project': project,
            'sub_project': None,
            'machine': cpt[start + 1].split()[-1][:-1],
            'node': 'standard',
            'consumption_date': dt.datetime.strptime(
                "{} {}".format(cpt[start].split()[-2:][0],
                               cpt[start].split()[-2:][1]),
                               "%d/%m/%Y %H:%M"
            ),
            'consumption': _get_consumption(cpt, start, end),
            'total': float(cpt[end].split()[1]),
            'project_allocation': float(cpt[start + 3].split()[-1]),
            'project_end_date': dt.datetime(_YEAR, 12, 31, 23, 59, 59),
            'project_start_date': dt.datetime(_YEAR, 01, 01)
            }


def _get_indexes(cpt):
    """Returns CPT block start/end positions.

    """
    return zip(
        [i for i, v in enumerate(cpt) if v.find('mise a jour') > -1],
        [i for i, v in enumerate(cpt) if v.find('totaux') > -1 and len(v.split()) == 4]
    )


def _get_consumption(cpt, start, end):
    return [(l[-5], float() if l[-3] == '-' else float(l[-3]))
            for l in [l.split() for l in cpt[start + 7: end - 1]]]
