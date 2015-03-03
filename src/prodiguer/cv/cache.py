# -*- coding: utf-8 -*-

"""
.. module:: cv.cache.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary cache manager.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import collections, random

from prodiguer.utils import rt
from prodiguer.cv import constants, io, accessor as ta



# Cached set of loaded cv's keyed by type.
_DATA = collections.defaultdict(list)


def _extend_term(term):
    """Extends a CV term with information derived from it's declared attributes.

    """
    ta.set_sort_key(term)


def sort():
    """Sorts cache.

    """
    for cv_type, collection in _DATA.iteritems():
        _DATA[cv_type] = sorted(collection, key=ta.get_sort_key)


def load(log=True):
    """Loads cache.

    """
    # Escape if already loaded.
    if _DATA:
        return

    # Read CV terms from file system.
    termset = io.read(log)

    # Extend terms.
    for term in termset:
        _extend_term(term)

    # Initialize cache.
    for term in termset:
        _DATA[ta.get_type(term)].append(term)

    # Sort terms.
    sort()


def reload():
    """Reloads cache.

    """
    if _DATA:
        rt.log_cv("RELOADING CV CACHE ...")
        _DATA.clear()
    load()


def add_term(term):
    """Adds a term to the cache.

    :param dict term: Term to be added to cache.

    """
    _DATA[ta.get_type(term)].append(term)


def remove_term(term):
    """Remvoes a term from the cache.

    :param dict term: Term to be removed to cache.

    """
    _DATA[ta.get_type(term)].remove(term)


def get_all_termsets():
    """Returns all cv term sets.

    :returns: List of 2 member tuples containing term type and sorted terms.
    :rtype: list

    """
    result = {}
    for term_type, termset in _DATA.iteritems():
        result[term_type] = { term['meta']['name']: term for term in termset }

    return result


def get_termset(term_type):
    """Returns set of cv term types.

    :returns: List of 2 member tuples containing term type and sorted terms.
    :rtype: list

    """
    term_type = unicode(term_type).lower()
    if term_type not in constants.TERM_TYPESET:
        raise ValueError("Unknown CV type :{}".format(term_type))

    return _DATA[term_type] if term_type in _DATA else []


def get_term(term_type, term_name):
    """Returns set of cv term types.

    :param term_type: Type of term being retrieved.
    :param term_name: Name of term being retrieved.

    :returns: List of 2 member tuples containing term type and sorted terms.
    :rtype: list

    """
    termset = get_termset(term_type)
    term_name = unicode(term_name).lower()
    for term in termset:
        if ta.get_name(term) == term_name:
            return term


def get_term_count():
    """Returns count of terms.

    :returns: Count of terms.
    :rtype: int

    """
    count = 0
    for term_type in _DATA.keys():
        count += len(_DATA[term_type])

    return count


def get_term_typeset():
    """Returns set of term types.

    :returns: Set of term types.
    :rtype: list

    """
    return sorted(_DATA.keys())


def get_random_term(term_type):
    """Returns a randomly selected term.

    :param term_type: Type of term being retrieved.

    :returns: A randomly selected term.
    :rtype: dict

    """
    termset = get_termset(term_type)

    return termset[random.randint(0, len(termset) - 1)]


def get_random_term_name(term_type):
    """Returns name of a randomly selected term.

    :param term_type: Type of term being retrieved.

    :returns: A randomly selected term name.
    :rtype: str

    """
    return ta.get_name(get_random_term(term_type))

