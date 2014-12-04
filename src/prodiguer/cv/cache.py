# -*- coding: utf-8 -*-

"""
.. module:: cv.cache.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary cache manager.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import glob, os, random
from os.path import dirname, abspath

from .. utils import convert



# Loaded set of cv's.
_cache = {}


def _is_matched_name(term, name):
    """Predicate determining whether a term has a matching name.

    """
    name = str(name).upper()
    names = [term['name']]
    try:
        names += term['synonyms'].split(",")
    except KeyError:
        pass
    else:
        names = [n for n in names if n]
        names = [n.strip() for n in names]
        names = [n for n in names if len(n)]
    names = [n.upper() for n in names]

    return name in names


def _get_as_namedtuple(term):
    """Converts a term to a namedtuple.

    """
    return convert.dict_to_namedtuple(term)


def sort():
    """Sorts cached CV collection.

    """
    for cv_type, collection in _cache.iteritems():
        _cache[cv_type] = sorted(collection, key=lambda i: i['name'])


def load():
    """Loads cache.

    """
    def _get_cv_type(fpath):
        """Returns cv type from a path to a CV file.

        """
        return fpath.split("/")[-1].split(".")[0]


    def _get_cv_terms(fpath):
        """Returns cv terms from a path to a CV file.

        """
        return convert.json_file_to_dict(fpath)


    if len(_cache) > 0:
        return

    # Get pointers to cv files.
    files = os.path.join(dirname(abspath(__file__)), "files")
    files = os.path.join(files, "*.json")
    files = sorted(glob.glob(files))
    files = [f for f in files if not f.endswith("simulation.json")]

    # Load cv terms from cv files.
    _cache.update({_get_cv_type(f) : _get_cv_terms(f) for f in files})

    # Sort.
    sort()


def get_all():
    """Returns all CV terms.

    :returns: All CV items.
    :rtype: list

    """
    result = []
    for cv_type in _cache.keys():
        for term in _cache[cv_type]:
            result.append(term)

    return result


def get_names(cv_type):
    """Returns set of a CV term names.

    :param str cv_type: Type of CV term.

    """
    if cv_type not in _cache:
        return None

    return [i['name'] for i in _cache[cv_type]]


def get_collection(cv_type, as_namedtuple=False):
    """Returns a CV collection.

    :param str cv_type: Type of CV term.
    :param bool as_namedtuple: Flag indicating whether to return the result in namedtuple format.

    """
    if cv_type not in _cache:
        return None
    collection = _cache[cv_type]

    if as_namedtuple:
        return [_get_as_namedtuple(t) for t in collection]
    else:
        return collection


def get_term(cv_type, term_id, as_namedtuple=False):
    """Returns a CV term.

    :param str cv_type: Type of CV term.
    :param str term_id: A CV term identifier.
    :param bool as_namedtuple: Flag indicating whether to return the result in namedtuple format.

    """
    collection = get_collection(cv_type)
    if collection is None:
        return None

    for term in collection:
        if _is_matched_name(term, term_id):
            if as_namedtuple:
                term = _get_as_namedtuple(term)
            return term


def get_term_name(cv_type, term_id):
    """Returns a CV term name.

    :param str cv_type: Type of CV term.
    :param str term_id: A CV term identifier.

    :returns: A cached CV term name.
    :rtype: str

    """
    term = get_term(cv_type, term_id)

    return None if term is None else term['name']


def get_types():
    """Returns set of cv types.

    """
    return sorted(_cache.keys())


def get_count():
    """Returns count of items in cache.

    :returns: Count of items in cache.
    :rtype: int

    """
    count = 0
    for cv_type in _cache.keys():
        count += len(_cache[cv_type])

    return count


def get_random_term(cv_type):
    """Returns a random cache item.

    :param str cv_type: Type of CV term.

    :returns: A randomly selected term.
    :rtype: dict

    """
    collection = _cache[cv_type]
    term_id = random.randint(0, len(collection) - 1)

    return collection[term_id]


def get_random_name(cv_type):
    """Returns a random cache item name.

    :param str cv_type: Type of CV term.

    :returns: A randomly selected term name.
    :rtype: str

    """
    return get_random_term(cv_type)['name']
