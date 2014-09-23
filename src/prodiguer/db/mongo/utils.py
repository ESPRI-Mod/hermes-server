# -*- coding: utf-8 -*-

"""
.. module:: prodiguer.db.mongo.utils.py
   :platform: Unix
   :synopsis: Set of mongo db related utilty functions.

.. moduleauthor:: Mark Conway-Greenslade (formerly Morgan) <momipsl@ipsl.jussieu.fr>


"""
import pymongo



def get_db(db_name):
    """Returns a pointer to a db.

    :param str db_name: Name of database to connect to.

    :returns: Pymongo pointer to MongoDB database.
    :rtype: pymongo.database.Database

    """
    # TODO get client connection string from config
    mg_client = pymongo.MongoClient()
    return mg_client[db_name]


def get_db_collection(db_name, collection_name):
    """Returns a pointer to a db collection.

    :param str db_name: Name of database to connect to.
    :param str collection_name: Name of collection to return a pointer to.

    :returns: Pymongo pointer to MongoDB collection.
    :rtype: pymongo.collection.Collection

    """
    mg_db = get_db(db_name)

    return mg_db[collection_name]


