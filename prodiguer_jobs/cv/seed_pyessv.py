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

import prodiguer
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
	"""Returns a pyessv.Term instantiated from an ipsl.prodiguer term.

	"""
	# Create a term.
	term = partition.create(kind, prodiguer.cv.get_name(data))
	term.aliases = prodiguer.cv.get_synonyms(data)
	term.create_date = prodiguer.cv.get_create_date(data)
	term.description = prodiguer.cv.get_description(data)
	term.id = idx
	term.uid = prodiguer.cv.get_uid(data)
	term.url = data.get("url")

	# Set UI label.
	term.set_label(prodiguer.cv.get_display_name(data))

	# Set term data.
	term.data = _get_term_data(data)

	# Implicitly mark as accepted.
	partition.accept(term)

	return term


def _yield_termset(partition):
	"""Yields set of pyessv terms to be written to file system by pyessv.

	"""
	prodiguer.cv.cache.load(log=False)
	cv_set = {i: prodiguer.cv.cache.get_termset(i) for i in prodiguer.cv.constants.TERM_TYPESET}
	for cv_type in cv_set:
		for idx, data in enumerate(cv_set[cv_type]):
			yield _create_term(partition, cv_type, idx + 1, data)


def _main():
	"""Main entry point.

	"""
	# Set pyessv partition.
	partition = pyessv.create_partition(
	    prodiguer.VOCAB_DOMAIN,
	    prodiguer.VOCAB_SUBDOMAIN,
	    os.getenv("PRODIGUER_PYESSV_HOME")
	)

	# Save set of CV terms.
	partition.save(_yield_termset)


# Main entry point.
if __name__ == '__main__':
    _main()
