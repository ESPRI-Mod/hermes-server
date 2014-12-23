# -*- coding: utf-8 -*-

"""
.. module:: cv.term_accessor.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary term access manager.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import arrow



def get_repr(term):
    """Returns a term string representation.

    """
    return "{0}.{1}".format(get_type(term), get_name(term))


def get_name(term):
    """Returns a term name.

    """
    return term['meta']['name']


def get_type(term):
    """Returns a term type.

    """
    return term['meta']['type']


def get_synonyms(term):
    """Returns a term's synonyms.

    """
    try:
        synonyms = term['synonyms']
    except KeyError:
        synonyms = []
    else:
        try:
            synonyms = synonyms.split(",")
        except AttributeError:
            pass
        synonyms = [n for n in synonyms if n and n.strip()]
        synonyms = [n.strip().lower() for n in synonyms]

    return synonyms


def get_sort_key(term):
    """Returns a term type.

    """
    return term['meta']['sort_key']


def set_sort_key(term):
    """Sets a term's sort key.

    """
    key = get_type(term)
    key += ":"
    if 'sort_key' in term:
        key += term['sort_key']
    elif 'sortKey' in term:
        key += term['sortKey']
    else:
        key += get_name(term)
    key = key.lower()
    term['meta']['sort_key'] = key


def set_status(term, status):
    """Sets a term's governance status.

    """
    term['meta']['status'] = status


def set_update_date(term):
    """Sets a term's update date.

    """
    term['meta']['update_date'] = unicode(arrow.utcnow())
