# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.sim_metrics.__init__.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metrics package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.web.endpoints.sim_metrics.add import AddRequestHandler
from prodiguer.web.endpoints.sim_metrics.delete import DeleteRequestHandler
from prodiguer.web.endpoints.sim_metrics.fetch import FetchRequestHandler
from prodiguer.web.endpoints.sim_metrics.fetch_columns import FetchColumnsRequestHandler
from prodiguer.web.endpoints.sim_metrics.fetch_count import FetchCountRequestHandler
from prodiguer.web.endpoints.sim_metrics.fetch_list import FetchListRequestHandler
from prodiguer.web.endpoints.sim_metrics.fetch_setup import FetchSetupRequestHandler
from prodiguer.web.endpoints.sim_metrics.rename import RenameRequestHandler
from prodiguer.web.endpoints.sim_metrics.set_hashes import SetHashesRequestHandler
