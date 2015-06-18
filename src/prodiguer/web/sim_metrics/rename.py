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

from prodiguer.web import utils_handler
from prodiguer.web.sim_metrics import utils
from prodiguer.db.mongo import dao_metrics as dao
from prodiguer.utils import rt



# Query parameter names.
_PARAM_GROUP = 'group'
_PARAM_NEW_NAME = 'new_name'


class RenameRequestHandler(tornado.web.RequestHandler):
    """Simulation metric group rename method request handler.

    """
    def _validate_request(self):
        """Validates request.

        """
        utils.validate_group_name(self.get_argument(_PARAM_GROUP))
        utils.validate_group_name(self.get_argument(_PARAM_NEW_NAME),
        						  validate_db_collection=False)


    def _decode_request(self):
        """Decodes request.

        """
        self.group = self.get_argument(_PARAM_GROUP)
        self.new_name = self.get_argument(_PARAM_NEW_NAME)


    def _rename_metric_group(self):
        """Renames metrics group within db.

        """
        dao.rename(self.group, self.new_name)


    def _write_response(self, error=None):
        """Write response output.

        """
        utils_handler.write_response(self, error)


    def _log(self, error=None):
        """Logs request processing completion.

        """
        utils_handler.log("metric", self, error)


    def post(self):
    	"""HTTP POST handler.

    	"""
        # Define tasks.
        tasks = {
            "green": (
                self._validate_request,
                self._decode_request,
                self._rename_metric_group,
                self._write_response,
                self._log,
                ),
            "red": (
                self._write_response,
                self._log,
                )
        }

        # Invoke tasks.
        rt.invoke(tasks)
