# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.metrics_pcmdi.__init__.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metrics package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.web.endpoints.metrics_pcmdi.add import AddRequestHandler
from prodiguer.web.endpoints.metrics_pcmdi.delete import DeleteRequestHandler
from prodiguer.web.endpoints.metrics_pcmdi.fetch import FetchRequestHandler
from prodiguer.web.endpoints.metrics_pcmdi.fetch_columns import FetchColumnsRequestHandler
from prodiguer.web.endpoints.metrics_pcmdi.fetch_count import FetchCountRequestHandler
from prodiguer.web.endpoints.metrics_pcmdi.fetch_list import FetchListRequestHandler
from prodiguer.web.endpoints.metrics_pcmdi.fetch_setup import FetchSetupRequestHandler
from prodiguer.web.endpoints.metrics_pcmdi.rename import RenameRequestHandler
from prodiguer.web.endpoints.metrics_pcmdi.set_hashes import SetHashesRequestHandler
