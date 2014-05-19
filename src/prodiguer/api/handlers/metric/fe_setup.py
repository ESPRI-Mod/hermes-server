# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.metric.setup.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import tornado.web

from . import utils
from .... import db
from .... utils import (
    config as cfg,
    convert
    )




class FrontEndSetupRequestHandler(tornado.web.RequestHandler):
    """Simulation metric front end setup request handler.

    """
    def set_default_headers(self):
        """Set HTTP headers at the beginning of the request."""
        self.set_header(utils.HTTP_HEADER_Access_Control_Allow_Origin,
                        ",".join(cfg.api.metric.cors_white_list))


    def get(self, *args):
        # Load setup data from db.
        data = {
            'group_list': utils.get_list(db.types.SimulationMetricGroup),
        };

        # Convert keys to camel case so as to respect json naming conventions.
        data = convert.dict_keys(data, convert.str_to_camel_case)

        self.write(data)
