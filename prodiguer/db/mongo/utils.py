# -*- coding: utf-8 -*-

"""
.. module:: hermes.db.mongo.utils.py
   :platform: Unix
   :synopsis: Set of mongo db related utilty functions.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import pymongo

from hermes.utils import config



def get_client(document_class=dict):
    """Returns a mongo db server connection.

    :param class document_class: Default class to use for documents returned from queries.

    :returns: Pymongo connection to MongoDB server.
    :rtype: pymongo.MongoClient

    """
    return pymongo.MongoClient(config.db.mongodb.main, document_class=document_class)


def get_db(db_name, document_class=dict):
    """Returns a pointer to a db.

    :param str db_name: Name of database to connect to.
    :param class document_class: Default class to use for documents returned from queries.

    :returns: Pymongo pointer to MongoDB database.
    :rtype: pymongo.database.Database

    """
    client = get_client(document_class)

    return client.get_database(db_name)


def get_db_collection(db_name, collection_name, document_class=dict):
    """Returns a pointer to a db collection.

    :param str db_name: Name of database to connect to.
    :param str collection_name: Name of collection to return a pointer to.
    :param class document_class: Default class to use for documents returned from queries.

    :returns: Pymongo pointer to MongoDB collection.
    :rtype: pymongo.collection.Collection

    """
    db = get_db(db_name, document_class)

    return db.get_collection(collection_name)
