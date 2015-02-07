# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.monitoring.setup.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.web

from prodiguer import db
from prodiguer.api.utils import handler as hu
from prodiguer.utils import config



class FrontEndSetupRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        # Start db session.
        db.session.start()

        # Load setup data from db.
        data = {
            'cv_terms': db.utils.get_list(db.types.ControlledVocabularyTerm),
            'simulation_list': db.utils.get_list(db.types.Simulation)
            }

        # End db session.
        db.session.end()

        # Write response.
        hu.write_json_response(self, data)
