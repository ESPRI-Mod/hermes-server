# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.monitoring.setup.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.web

from prodiguer import db
from prodiguer.api.handlers.monitoring import utils
from prodiguer.utils import config, convert




class FrontEndSetupRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        # Connect to db.
        db.session.start(config.db.pgres.main)

        # Load setup data from db.
        self.write(convert.dict_keys({
            'cv_terms':
                utils.get_sorted_list(db.types.CvTerm),
            'simulation_list':
                utils.get_list(db.types.Simulation),
            'simulation_state_change_list':
                utils.get_list(db.types.SimulationStateChange)
        }, convert.str_to_camel_case))
