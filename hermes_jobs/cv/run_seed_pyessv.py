# -*- coding: utf-8 -*-

"""
.. module:: seed_pyessv.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Seeds cv archive with pyessv formatted terms.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import os

import hermes
import pyessv


# Set of non-data related term keys.
_NON_DATA_KEYS = {'meta', 'description', 'name'}


def _get_term_data(data):
	"""Returns custom data associated with a term.

	"""
	for key in _NON_DATA_KEYS:
		try:
			del data[key]
		except KeyError:
			pass

	return data


def _create_term(partition, kind, idx, data):
	"""Returns a pyessv.Term instantiated from an ipsl.hermes term.

	"""
	# Create a term.
	term = partition.create(kind, hermes.cv.get_name(data))
	term.aliases = hermes.cv.get_synonyms(data)
    if term.aliases:
        term.alternative_name = term.aliases[0]
	term.create_date = hermes.cv.get_create_date(data)
	term.description = hermes.cv.get_description(data)
	term.id = idx
	term.uid = hermes.cv.get_uid(data)
	term.url = data.get("url")

	# Set UI label.
	term.set_label(hermes.cv.get_display_name(data))

	# Set term data.
	term.data = _get_term_data(data)

	# Implicitly mark as accepted.
	partition.accept(term)

	return term


def _yield_termset(partition):
	"""Yields set of pyessv terms to be written to file system by pyessv.

	"""
	hermes.cv.cache.load(log=False)
	cv_set = {i: hermes.cv.cache.get_termset(i) for i in hermes.cv.constants.TERM_TYPESET}
	for cv_type in cv_set:
		for idx, data in enumerate(cv_set[cv_type]):
			yield _create_term(partition, cv_type, idx + 1, data)


# Expose to pyessv seeder.
yield_terms = _yield_termset


def execute():
	"""Main entry point.

	"""
	# Set pyessv partition.
	partition = pyessv.create_partition(
	    hermes.VOCAB_DOMAIN,
	    os.getenv("PYESSV_HOME")
	)

	# Save set of CV terms.
	partition.save(_yield_termset)


# Main entry point.
if __name__ == '__main__':
    execute()
