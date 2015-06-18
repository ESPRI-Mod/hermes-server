# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.monitoring.__init__.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring package initializer.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.web.monitoring.event import EventRequestHandler
from prodiguer.web.monitoring.fe_cv import FrontEndControlledVocabularyRequestHandler
from prodiguer.web.monitoring.fe_setup_all import FrontEndSetupAllRequestHandler
from prodiguer.web.monitoring.fe_setup_one import FrontEndSetupOneRequestHandler
from prodiguer.web.monitoring.fe_ws_all import FrontEndWebSocketAllHandler
