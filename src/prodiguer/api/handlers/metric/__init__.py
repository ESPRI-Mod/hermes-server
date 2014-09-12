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
from .delete import DeleteRequestHandler
from .delete_lines import DeleteLinesRequestHandler
from .fetch import FetchRequestHandler
from .fetch_columns import FetchColumnsRequestHandler
from .fetch_count import FetchCountRequestHandler
from .fetch_setup import FetchSetupRequestHandler
from .fe_setup import FrontEndSetupRequestHandler
from .list import ListRequestHandler
