# -*- coding: utf-8 -*-

"""
.. module:: cv.io.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary file system io manager.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from os.path import dirname, abspath

from .. import db
from .. utils import convert



def get_filename(cv_type):
    """Returns path to cv json file.

    :param cv_type: CV entity type.
    :type cv_type: class | str

    :returns: CV file name.
    :rtype: str

    """
    try:
        return "{0}_{1}.json".format(
            cv_type.__table__.schema,
            convert.str_to_underscore_case(cv_type.__name__))
    except AttributeError:
        return "shared_cvterm.{}.json".format(cv_type.lower())


def get_filepath(cv_type):
    """Returns path to cv json file.

    :param cv_type: CV entity type.
    :type cv_type: class | str

    :returns: CV filepath.
    :rtype: str

    """
    return "{0}/json/{1}".format(dirname(abspath(__file__)),
                                 get_filename(cv_type))


def list_types():
    """Lists the set of supported cv types.

    """
    for cv_type in db.types.CV:
        yield cv_type.__table__.schema, cv_type.__name__


def write(targets=db.types.CV):
    """Writes json files to the file system.

    :params targets: A set of model types that have an associated cv.
    :type targets: list

    """
    from .. import db

    for cv_type in db.types.CV:
        if cv_type in targets:
            instances = db.dao.get_all(cv_type)
            as_json = db.types.Convertor.to_json(instances)
            with open(get_filepath(cv_type), 'w') as out_file:
                out_file.write(as_json)


def read(cv_type):
    """Reads a set of cv terms from associated json file.

    :param cv_type: CV entity type.
    :type cv_type: class | str

    :returns: Loaded CV.
    :rtype: list of dict

    """
    return convert.json_file_to_dict(get_filepath(cv_type))


def load(cv_type):
    """Loads a set of cv type instances from associated json file.

    :param cv_type: CV entity type.
    :type cv_type: class | str

    :returns: Loaded collection of entity type instances.
    :rtype: list of entity type instances

    """
    terms = read(cv_type)

    return db.types.Convertor.from_dict(cv_type, terms)
