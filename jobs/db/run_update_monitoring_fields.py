# -*- coding: utf-8 -*-

"""
.. module:: update_monitoring_fields.py
   :copyright: Copyright "Apr 26, 2013", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Ensures that all simulation fields related to monitoring are assigned.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import base64
import ConfigParser
import io

import prodiguer.db.pgres as db


def _decode_base64_card(card):
	return base64.decodestring(card)


def _main():
    """Main entry point.

    """
    db.session.start()

    for simulation in db.dao.get_all(db.types.Simulation):
    	configuration = db.dao_monitoring.retrieve_simulation_configuration(simulation.uid)
    	if configuration is None or configuration.card is None:
    		continue

    	print simulation.uid

    	card = _decode_base64_card(configuration.card)
    	print card

    	config = ConfigParser.RawConfigParser(allow_no_value=True)
    	config.readfp(io.BytesIO(card))

    	print dir(config)
    	print config.sections()
    	print config.get('UserChoices', 'ExpType')

    	break

    db.session.end()


# Main entry point.
if __name__ == '__main__':
    _main()
