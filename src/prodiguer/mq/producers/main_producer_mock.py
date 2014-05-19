# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.mocks.pub_cc_to_bkr.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Mocks publishing messages from computing centre to broker.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
import datetime
import random
import time
import uuid

from .. import utils
from ... import db
from ... utils import (
	config as cfg,
	runtime as rt
	)



# Target message queue.
MQ = "main"


# Simulation list.
_s_list = []


def _get_name(type):
	return db.cache.get_random_name(type)


def _log(msg):
	rt.log_mq(msg, cfg.mq.host, MQ)


def _get_msg_for_new_simulation():
	return {
		'event_type': 'new',
		'activity': _get_name(db.types.Activity),
		'compute_node': _get_name(db.types.ComputeNode),
		'compute_node_login': _get_name(db.types.ComputeNodeLogin),
		'compute_node_machine': _get_name(db.types.ComputeNodeMachine),
		'execution_start_date': datetime.datetime.now(),
		'execution_state': _get_name(db.types.SimulationState),
		'experiment': _get_name(db.types.Experiment),
		'model': _get_name(db.types.Model),
		'name': "test-" + str(uuid.uuid4())[:6],
		'space': _get_name(db.types.SimulationSpace)
	}


def _get_msg_for_simulation_state_change(s_name):
	idx = random.randint(0, len(db.constants.EXECUTION_STATE_SET) - 1)
	return {
		'event_type': 'state_change',
		'name': s_name,
		'state': db.constants.EXECUTION_STATE_SET[idx]
	}


def _publish_new_simulation():
	msg = _get_msg_for_new_simulation()
	_s_list.append(msg['name'])
	_log("PUBLISHING :: new simulation ({0})".format(msg['name']))
	utils.publish(MQ, msg)
	_log("PUBLISHED :: new simulation ({0})".format(msg['name']))


def _publish_simulation_state_change(s_name):
	msg = _get_msg_for_simulation_state_change(s_name)
	_log("PUBLISHING :: simulation state change")
	utils.publish(MQ, msg)
	_log("PUBLISHED :: simulation state change")


def _publish():
	# publish new.
	if random.randint(0, 3) == 0:
		_publish_new_simulation()
		
	# publish state updates.
	else:
		for s_name in random.sample(_s_list, 2):
			_publish_simulation_state_change(s_name)


def _publish_forever():
	while True:
		time.sleep(1)
		_publish()


def _publish_thrice():
	for i in range(3):
		time.sleep(1)
		_publish()


def start(forever = True):
	for s in [s for s in db.dao.get_all(db.types.Simulation) if s.name.startswith('v3')]:
		_s_list.append(s.name)
	if forever:
		_publish_forever()
	else:
		_publish_thrice()
