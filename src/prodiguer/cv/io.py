# -*- coding: utf-8 -*-

"""
.. module:: TODO - write module name
   :copyright: Copyright "May 22, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: TODO - write synopsis

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from os.path import dirname, abspath

from .. import db
from .. utils import convert



# Default character set.
_JSON_CHARSET = "ISO-8859-1"


def get_filename(etype):
    """Returns path to cv json file.

    :param etype: CV entity type.
    :type etype: class

    :returns: CV file name.
    :rtype: str

    """
    db.types.assert_type(etype)

    return "{0}_{1}.json".format(
        etype.__table__.schema, 
        convert.str_to_underscore_case(etype.__name__))


def get_filepath(etype):
    """Returns path to cv json file.

    :param etype: CV entity type.
    :type etype: class

    :returns: CV filepath.
    :rtype: str

    """
    db.types.assert_type(etype)

    return "{0}/json/{1}".format(dirname(abspath(__file__)), 
                                 get_filename(etype))


def list_types():
    """Lists the set of supported cv types."""
    for etype in db.types.CV:
        yield etype.__table__.schema, etype.__name__


def write(targets=db.types.CV):
    """Writes json files to the file system.

    :params targets: A set of model types that have an associated cv.
    :type targets: list
    
    """
    from .. import db
    
    for etype in db.types.CV:
        if etype in targets:
            instances = db.dao.get_all(etype)
            as_json = db.types.Convertor.to_json(instances)
            with open(get_filepath(etype), 'w') as f: 
                f.write(as_json)


def read(etype):
    """Reads a set of cv terms from associated json file.

    :param etype: CV entity type.
    :type etype: class

    :returns: Loaded CV.
    :rtype: list of dict

    """
    db.types.assert_type(etype)

    return convert.json_file_to_dict(get_filepath(etype))


def load(etype):
    """Loads a set of cv type instances from associated json file.

    :param etype: CV entity type.
    :type etype: class

    :returns: Loaded collection of entity type instances.
    :rtype: list of entity type instances

    """
    db.types.assert_type(etype)

    d = convert.json_file_to_dict(get_filepath(etype))

    return db.types.Convertor.from_dict(etype, d)

