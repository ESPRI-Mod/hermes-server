# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.meta.py
   :platform: Unix
   :synopsis: Database ORM metadata singleton.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
from sqlalchemy import MetaData



# Sqlalchemy metadata singleton.
METADATA = MetaData()
