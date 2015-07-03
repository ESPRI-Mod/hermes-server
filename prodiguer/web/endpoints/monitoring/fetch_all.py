# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.monitoring.fetch_all.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from prodiguer.web.utils import ProdiguerHTTPRequestHandler
from prodiguer.web.endpoints.monitoring import _validator as validator
from prodiguer.db import pgres as db



class FetchAllRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation monitoring all simulations front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _get_data(func):
            """Returns data for front-end.

            """
            return db.utils.get_collection(func())


        def _set_output():
            """Sets response to be returned to client.

            """
            db.session.start()
            self.output = {
                'job_history':
                    _get_data(db.dao_monitoring.retrieve_active_jobs),
                'simulation_list':
                    _get_data(db.dao_monitoring.retrieve_active_simulations)
            }
            db.session.end()

        # Invoke tasks.
        self.invoke(validator.validate_fetch_all, _set_output)
