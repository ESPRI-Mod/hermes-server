# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.sim_metrics.__init__.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metrics package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.web.sim_metrics.add import AddRequestHandler
from prodiguer.web.sim_metrics.delete import DeleteRequestHandler
from prodiguer.web.sim_metrics.fetch import FetchRequestHandler
from prodiguer.web.sim_metrics.fetch_columns import FetchColumnsRequestHandler
from prodiguer.web.sim_metrics.fetch_count import FetchCountRequestHandler
from prodiguer.web.sim_metrics.fetch_list import FetchListRequestHandler
from prodiguer.web.sim_metrics.fetch_setup import FetchSetupRequestHandler
from prodiguer.web.sim_metrics.rename import RenameRequestHandler
from prodiguer.web.sim_metrics.set_hashes import SetHashesRequestHandler
