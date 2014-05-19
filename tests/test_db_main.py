# -*- coding: utf-8 -*-

"""
.. module:: test_db.py

   :copyright: @2013 Institute Pierre Simon Laplace (http://esdocumentation.org)
   :license: GPL / CeCILL
   :platform: Unix
   :synopsis: Encapsulates db tests.

.. moduleauthor:: Institute Pierre Simon Laplace (ES-DOC) <dev@esdocumentation.org>

"""
import inspect



def test_db_import_package():
    from prodiguer import db

    modules = [
        db,
        db.cache,
        db.dao,
        db.session,
        db.setup,
        db.type_factory,
    ]
    for module in modules:
        assert inspect.ismodule(module)        
