# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.api.handlers.monitoring.setup.py
   :copyright: Copyright "Feb 7, 2013", Earth System Documentation
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation monitoring front end setup request handler.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import tornado.web

from . import utils
from .... import db
from ....utils import convert




def _get_simulation_list():
    s_list = db.dao.get_all(db.types.Simulation)
    s_list = [s for s in s_list if s.name.startswith("v3")]

    return [utils.get_simulation_dict(s) for s in s_list]


class FrontEndSetupRequestHandler(tornado.web.RequestHandler):
    """Simulation monitoring front end setup request handler.

    """
    def get(self, *args):
        # Load setup data from db.
        data = {
            'activity_list': utils.get_list(db.types.Activity),
            'compute_node_list': utils.get_list(db.types.ComputeNode),
            'compute_node_machine_list': utils.get_list(db.types.ComputeNodeMachine),
            'compute_node_login_list': utils.get_list(db.types.ComputeNodeLogin),
            'experiment_list': utils.get_list(db.types.Experiment),
            'model_list': utils.get_list(db.types.Model),
            'simulation_list': _get_simulation_list(),
            'execution_state_list': utils.get_list(db.types.SimulationState),
            'space_list': utils.get_list(db.types.SimulationSpace),
        };

        # Convert keys to camel case so as to respect json naming conventions.
        data = convert.dict_keys(data, convert.str_to_camel_case)

        self.write(data)

