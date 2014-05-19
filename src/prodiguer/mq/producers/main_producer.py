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
import uuid

from .. import constants, utils
from ... import db



def _get_name(type):
	return db.cache.get_random_name(type)


def _get_slist():
	return [s.name for s in db.dao.get_all(db.types.Simulation) if s.name.startswith('v3')]


class _BaseSimulationMonitorMessageProducer(utils.BaseProducer):
	"""Base class for simulation montioring message publishers.

	"""
	APP = constants.APP_SMON
	MQ_QUEUE = 'libligcm.sim-mon'
	MQ_ROUTING_KEY = 'libligcm.sim-mon'
	PRODUCER = constants.PRODUCER_IGCM


class MessageType1000Producer(_BaseSimulationMonitorMessageProducer):
	"""Simulation monitoring message publisher (message type = 1000 = new simulation).

	"""
	MESSAGE_TYPE = constants.TYPE_SMON_1000
	PUBLISH_FREQUENCY = 3		# Publishing frequency randomizes when publishing will occur.

	def get_message(self):
		"""Publishes a message once in every 3 times it is called."""
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


class MessageType2000Producer(_BaseSimulationMonitorMessageProducer):
	"""Simulation monitoring message publisher (message type = 2000 = state change).

	"""
	MESSAGE_TYPE = constants.TYPE_SMON_2000

	def __init__(self, s_name):
		super(MessageType2000Producer, self).__init__()

		self.s_name = s_name


	def get_message(self):
		"""Publishes a message once in every 3 times it is called."""
		idx = random.randint(0, len(db.constants.EXECUTION_STATE_SET) - 1)

		return {
			'event_type': 'state_change',
			'name': self.s_name,
			'state': db.constants.EXECUTION_STATE_SET[idx]
		}


class MainProducer(utils.BaseProducer):
	APP = constants.APP_SMON
	MQ_QUEUE = 'libligcm.sim-mon'
	MQ_ROUTING_KEY = 'libligcm.sim-mon'
	PRODUCER = constants.PRODUCER_IGCM
	PUBLISH_LIMIT = 10
	PUBLISH_INTERVAL = 1


	def __init__(self):
		super(MainProducer, self).__init__()

		self.slist = _get_slist()


	def get_message(self):
		# New.		
		yield MessageType1000Producer()

		# State changes.
		for s_name in random.sample(self.slist, 3):
			yield MessageType2000Producer(s_name)



# Functional style.
def _get_2000_message(s_name):	
	"""Returns a message of type 2000."""
	def get_msg():
		idx = random.randint(0, len(db.constants.EXECUTION_STATE_SET) - 1)
		return {
			'event_type': 'state_change',
			'name': s_name,
			'state': db.constants.EXECUTION_STATE_SET[idx]
		}

	return MessageProducer(constants.TYPE_SMON_2000, get_msg)
	