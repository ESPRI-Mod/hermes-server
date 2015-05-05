# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.metric.__init__.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.web.metric.add import AddRequestHandler
from prodiguer.web.metric.delete import DeleteRequestHandler
from prodiguer.web.metric.fetch import FetchRequestHandler
from prodiguer.web.metric.fetch_columns \
	import FetchColumnsRequestHandler
from prodiguer.web.metric.fetch_count import FetchCountRequestHandler
from prodiguer.web.metric.fetch_list import FetchListRequestHandler
from prodiguer.web.metric.fetch_setup import FetchSetupRequestHandler
from prodiguer.web.metric.rename import RenameRequestHandler
from prodiguer.web.metric.set_hashes import SetHashesRequestHandler
