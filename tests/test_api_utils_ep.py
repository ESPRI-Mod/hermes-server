# -*- coding: utf-8 -*-

"""
.. module:: test_api_utils_ep_api.py

   :copyright: @2013 Institute Pierre Simon Laplace (http://esdocumentation.org)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates api endpoint utils tests.

.. moduleauthor:: Institute Pierre Simon Laplace (ES-DOC) <dev@esdocumentation.org>

"""
from . import utils as tu
from prodiguer.api.handlers import utils as handler_utils



_TEST_EP = r"http://localhost:8888/api/1/XXX"
_TEST_EP_SUFFIX = "/XXX"



def test_api_ep_get_endpoint():	
	tu.assert_string(handler_utils.get_endpoint(_TEST_EP_SUFFIX), _TEST_EP)
