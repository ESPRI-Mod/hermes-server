# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.pub_bkr_to_api.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Mocks publishing messages from broker to front-end.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from .. import utils
from ... utils import (
	runtime as rt, 
	config as cfg
	)


# Message queue to which I am bound.
MQ = "api"


def _log(msg):
	"""Outputs message ot mq log.""" 
	rt.log_mq(msg, cfg.mq.host, MQ)


def get_event_info_for_new(s):
	"""Returns new simulation event information."""
	return {
		'event_type': 'new',
		'activity_id': s.activity_id,
		'compute_node_id': s.compute_node_id,
		'compute_node_login_id': s.compute_node_login_id,
		'compute_node_machine_id': s.compute_node_machine_id,
		'execution_start_date': s.execution_start_date,
		'execution_state_id': s.execution_state_id,
		'experiment_id': s.experiment_id,
		'id': s.id,
		'model_id': s.model_id,
		'name': s.name,
		'space_id': s.space_id
	}


def get_event_info_for_state_change(id, state):
	"""Returns state change event information."""
	return {
		'event_type': 'state_change',
		'id': id,
		'state': state
	}


def do_new_simulation(s):
	"""Publishes details of a new simulation to the api message queue.

	:param s: New simulation.
	:type s: prodiguer.db.types.Simulation

	"""
	_log("PUBLISHING :: new simulation ({0})".format(s.name))
	
	ei = get_event_info_for_new(s)
	utils.publish(MQ, ei)

	_log("PUBLISHED :: new simulation ({0})".format(s.name))


def do_simulation_state_change(id, state):
	"""Publishes details of a simulation state change to the api message queue.

	:param id: Simulation id.
	:type id: int

	:param state: Exeuction status.
	:type state: str

	"""
	_log("PUBLISHING :: simulation state change")

	ei = get_event_info_for_state_change(id, state)
	utils.publish(MQ, ei)

	_log("PUBLISHED :: simulation state change")
