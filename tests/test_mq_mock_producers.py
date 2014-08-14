# -*- coding: utf-8 -*-

"""
.. module:: test_mq_mock_producers.py

   :copyright: @2013 Institute Pierre Simon Laplace (http://esdocumentation.org)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates mq mock publisher testing.

.. moduleauthor:: Institute Pierre Simon Laplace (ES-DOC) <dev@esdocumentation.org>

"""
import inspect

from prodiguer.mq import constants
from prodiguer.mq.producers import cc_smon

from . import (
    utils as tu,
    utils_mq
    )



def test_module_import():
    """Test module imports"""
    assert inspect.ismodule(cc_smon)


def test_cc_smon_messages():
    """Test simulation monitoring messages"""
    # Map of event types to message types.
    msg_types = {
        'new': constants.TYPE_SMON_1000,
        'state_change': constants.TYPE_SMON_2000
    }

    # Map of event types to content keys.
    content_keys = {
        'new': (
            'activity',
            'compute_node',
            'compute_node_login',
            'compute_node_machine',
            'execution_start_date',
            'execution_state',
            'experiment',
            'model',
            'name',
            'space'
            ),
        'state_change': (
            'name',
            'state'
            )
    }

    # Assert yielded mesages.
    yielded = 0
    for props, content in cc_smon.yield_mock_messages():
        yielded += 1
        tu.assert_dict(content, ('event_type', ))
        tu.assert_dict(content, content_keys[content['event_type']])
        utils_mq.assert_msg_props(props, \
                                  type_id=msg_types[content['event_type']])

    assert yielded == 4


def test_cc_smon_producer():
    """Test simulation monitoring message producer"""
    cc_smon.Producer.run()