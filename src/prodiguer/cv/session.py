# -*- coding: utf-8 -*-

"""
.. module:: cv.session.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary session manager.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import collections

from prodiguer.cv import cache, constants, io, factory, term_accessor as ta
from prodiguer.utils import rt



# Session state.
_STATE = collections.defaultdict(list)


def init():
    """Initializes a CV session.

    """
    cache.load()
    rt.log_cv("initialized CV session ...")


def commit():
    """Commits CV session changes.

    """
    for term in _STATE["destructions"]:
        io.delete(term)
        cache.remove_term(term)
        rt.log_cv("Destroyed term: {}".format(ta.get_repr(term)))
    for term in _STATE["insertions"]:
        io.write(term)
        cache.add_term(term)
        rt.log_cv("Inserted term: {}".format(ta.get_repr(term)))
    for term in _STATE["deletions"]:
        io.write(term)
        cache.remove_term(term)
        rt.log_cv("Deleted term: {}".format(ta.get_repr(term)))

    rt.log_cv("Session updates committed to file system")

    _STATE.clear()


def insert(term_type, term_name, term_data=None):
    """Marks a term for creatom.

    :param str term_type: Type of term being created.
    :param str term_name: Name of term being created.
    :param dict term_data: Associated term data.

    :returns: Newly created term.
    :rtype: dict

    """
    term = factory.create1(term_type, term_name, term_data)
    _STATE["insertions"].append(term)

    return term


def delete(term):
    """Marks a term for deletion.

    :param dict term: Term being deleted.

    """
    ta.set_status(term, constants.TERM_GOVERNANCE_STATE_DELETED)
    ta.set_update_date(term)
    _STATE["deletions"].append(term)


def destroy(term):
    """Marks a term for desctruction.

    :param dict term: Term being destroyed.

    """
    ta.set_status(term, constants.TERM_GOVERNANCE_STATE_DESTROYED)
    ta.set_update_date(term)
    _STATE["destructions"].append(term)
