# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.web.sim_metrics.rename.py
   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL/CeCIL
   :platform: Unix, Windows
   :synopsis: Simulation metric group rename request handler.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import tornado

from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import _utils as utils



# Query parameter names.
_PARAM_GROUP = 'group'


class SetHashesRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group set hashes method request handler.

    """
    def post(self):
        """HTTP POST handler.

        """
        def _do_work(self):
            """Sets the hash identifiers for all metrics within the group.

            """
            dao.set_hashes(self.get_argument(_PARAM_GROUP))

        # Invoke tasks.
        utils_handler.invoke(self, validator.validate_set_hashes, _do_work)
