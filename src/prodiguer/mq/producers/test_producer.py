# -*- coding: utf-8 -*-
from .. import constants, utils



class TestProducer(utils.BaseProducer):
	APP = constants.APP_SMON
	MQ_QUEUE = 'primary'
	MQ_ROUTING_KEY = 'igcm.sim-mon'
	PRODUCER = constants.PRODUCER_IGCM

	def get_message(self):
		yield constants.TYPE_SMON_1000, {
			u'مفتاح': u' قيمة',
			u'键': u'值',
			u'キー': u'値'
			}



