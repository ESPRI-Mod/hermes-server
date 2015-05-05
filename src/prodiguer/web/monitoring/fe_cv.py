# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.monitoring.setup.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado.web

from prodiguer.web import utils_handler
from prodiguer.db import pgres as db



class FrontEndControlledVocabularyRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring front end controlled vocabulary setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        # Start db session.
        db.session.start()

        # Load cv data from db.
        data = {
            'cv_terms':
                db.utils.get_list(db.types.ControlledVocabularyTerm)
            }

        # End db session.
        db.session.end()

        # Write response.
        utils_handler.write_json_response(self, data)
