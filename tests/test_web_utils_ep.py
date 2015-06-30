# -*- coding: utf-8 -*-

"""
.. module:: test_api_utils_ep_api.py

   :copyright: @2015 IPSL (http://ipsl.fr)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates api endpoint utils tests.

.. moduleauthor:: IPSL (ES-DOC) <dev@esdocumentation.org>

"""
from . import utils as tu
from prodiguer.web import utils_handler



_TEST_EP = r"http://localhost:8888/api/1/XXX"
_TEST_EP_SUFFIX = "/XXX"



def test_api_ep_get_endpoint():	
	tu.assert_string(utils_handler.get_endpoint(_TEST_EP_SUFFIX), _TEST_EP)
