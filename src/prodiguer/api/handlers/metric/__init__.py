# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.metric.__init__.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.api.handlers.metric.add import AddRequestHandler
from prodiguer.api.handlers.metric.delete import DeleteRequestHandler
from prodiguer.api.handlers.metric.fetch import FetchRequestHandler
from prodiguer.api.handlers.metric.fetch_columns \
	import FetchColumnsRequestHandler
from prodiguer.api.handlers.metric.fetch_count import FetchCountRequestHandler
from prodiguer.api.handlers.metric.fetch_list import FetchListRequestHandler
from prodiguer.api.handlers.metric.fetch_setup import FetchSetupRequestHandler
