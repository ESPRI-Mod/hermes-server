# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.endpoints.monitoring.fetch_one.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import base64

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring as dao
from prodiguer.web.request_validation import validator_monitoring as rv
from prodiguer.web.utils.http import ProdiguerHTTPRequestHandler



# Query parameter names.
_PARAM_HASHID = 'hashid'
_PARAM_TRYID = 'tryID'



class FetchOneRequestHandler(ProdiguerHTTPRequestHandler):
    """Simulation monitor front end setup request handler.

    """
    def get(self, *args):
        """HTTP GET handler.

        """
        def _decode_request():
            """Decodes request.

            """
            self.hashid = self.get_argument(_PARAM_HASHID)
            self.try_id = self.get_argument(_PARAM_TRYID)
            
        
        def _set_data():
            """Pulls data from db.
            
            """
            db.session.start()
            self.simulation = dao.retrieve_simulation_try(self.hashid, self.try_id)
            self.simulation_configuration = dao.retrieve_simulation_configuration(self.simulation.uid)
            self.job_history = dao.retrieve_simulation_jobs(self.simulation.uid)
            db.session.end()


        def _get_configuration_card():
            """Returns simulation configuration card formatted for front-end.

            """
            if self.simulation_configuration:                
                return base64.b64decode(self.simulation_configuration.card)
            else:
                return unicode()


        def _set_output():
            """Sets response to be returned to client.

            """
            self.output = {
                'job_history': db.utils.get_collection(self.job_history),
                'simulation': db.utils.get_item(self.simulation),
                'config_card': _get_configuration_card()
            }


        # Invoke tasks.
        self.invoke(rv.validate_fetch_one, [
            _decode_request,
            _set_data,
            _set_output
        ])
