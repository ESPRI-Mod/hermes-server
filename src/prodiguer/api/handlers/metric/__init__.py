# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.metric.__init__.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric package initializer.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from .add import AddRequestHandler
from .delete_lines import DeleteLinesRequestHandler
from .delete_group import DeleteGroupRequestHandler
from .fetch import FetchRequestHandler
from .fe_setup import FrontEndSetupRequestHandler
from .list_group import ListGroupRequestHandler
