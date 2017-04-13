# -*- coding: utf-8 -*-

"""
.. module:: hermes.web.endpoints.metrics_pcmdi.__init__.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metrics package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from hermes.web.endpoints.metrics_pcmdi.add import AddRequestHandler
from hermes.web.endpoints.metrics_pcmdi.delete import DeleteRequestHandler
from hermes.web.endpoints.metrics_pcmdi.fetch import FetchRequestHandler
from hermes.web.endpoints.metrics_pcmdi.fetch_columns import FetchColumnsRequestHandler
from hermes.web.endpoints.metrics_pcmdi.fetch_count import FetchCountRequestHandler
from hermes.web.endpoints.metrics_pcmdi.fetch_list import FetchListRequestHandler
from hermes.web.endpoints.metrics_pcmdi.fetch_setup import FetchSetupRequestHandler
from hermes.web.endpoints.metrics_pcmdi.rename import RenameRequestHandler
from hermes.web.endpoints.metrics_pcmdi.set_hashes import SetHashesRequestHandler
