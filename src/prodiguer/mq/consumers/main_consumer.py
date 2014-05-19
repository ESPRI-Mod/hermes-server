# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.mq.sub_bkr_to_fe.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Subscribes to front-end messages received from broker.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
# Module imports.
from ... import db
from .. import controller



# Main message queue.
MQ = "main"

# API message queue.
MQ_API = "api"


def _on_new(ei):
	"""On new simulation event handler."""
	# Persist simulation to db.	
	s = db.mq_hooks.create_simulation(
		ei['activity'],
		ei['compute_node'],
		ei['compute_node_login'],
		ei['compute_node_machine'],
		ei['execution_start_date'],
		ei['execution_state'],
		ei['experiment'],
		ei['model'],
		ei['name'],
		ei['space']
		)

	# Publish simulation to api.	
	controller.produce(MQ_API, 'do_new_simulation', s)


def _on_state_change(ei):
	"""On simulation state change event handler."""
	# Persist simulation state change to db.	
	s = db.mq_hooks.update_simulation_status(
		ei['name'],
		ei['state']
		)

	# Publish simulation state change to api.	
	controller.produce(MQ_API, 'do_simulation_state_change', s.id, ei['state'])


# Hash of event type handlers.
_msg_handlers = {
	'new': _on_new,
	'state_change': _on_state_change
}


def consume(msg):
	"""Message consumption entry point."""
	_msg_handlers[msg['event_type']](msg)