# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.metric.__init__.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.api.metric.add import AddRequestHandler
from prodiguer.api.metric.delete import DeleteRequestHandler
from prodiguer.api.metric.fetch import FetchRequestHandler
from prodiguer.api.metric.fetch_columns \
	import FetchColumnsRequestHandler
from prodiguer.api.metric.fetch_count import FetchCountRequestHandler
from prodiguer.api.metric.fetch_list import FetchListRequestHandler
from prodiguer.api.metric.fetch_setup import FetchSetupRequestHandler
from prodiguer.api.metric.rename import RenameRequestHandler
from prodiguer.api.metric.set_hashes import SetHashesRequestHandler
