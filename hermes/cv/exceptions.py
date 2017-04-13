# -*- coding: utf-8 -*-

"""
.. module:: cv.exceptions.py
   :copyright: Copyright "December 01, 2014", IPSL
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Controlled vocabulary package exceptions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""

class TermUIDError(Exception):
    """Error thrown when encountering an invalid term uid.

    """
    def __init__(self, uid):
        """Object constructor.

        """
        self.uid = uid
        msg = "Unknown CV term uid :: {}".format(uid)
        super(TermUIDError, self).__init__(msg)


class TermTypeError(Exception):
    """Error thrown when encountering an unknown term type.

    """
    def __init__(self, typeof):
        """Object constructor.

        """
        self.typeof = typeof
        msg = "Unknown CV term type :: {}".format(typeof)
        super(TermTypeError, self).__init__(msg)


class TermNameError(Exception):
    """Error thrown when encountering an unknown term name.

    """
    def __init__(self, typeof, name):
        """Object constructor.

        """
        self.typeof = typeof
        self.name = name
        msg = "Unknown CV term :: {0}.{1}".format(typeof, name)
        super(TermNameError, self).__init__(msg)


class TermUserDataError(Exception):
    """Error thrown when encountering invalid term user data.

    """
    def __init__(self):
        """Object constructor.

        """
        msg = "Invalid CV term user data"
        super(TermUserDataError, self).__init__(msg)
