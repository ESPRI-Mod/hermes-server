# -*- coding: utf-8 -*-

"""
.. module:: conso_project.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring 7010 messages (resource consumption metrics).

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import base64
import hashlib
import os

import hermes_cpt as hcpt
from hermes import mq
from hermes.utils import convert
from hermes.utils import config
from hermes.utils import logger



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
        _unpack,
        _validate,
        _parse,
        _write
    )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True, validate_props=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(props, body, decode=decode, validate_props=validate_props)

        self.centre = None
        self.cpt = None
        self.date = None
        self.output_dir = config.apps.conso.outputDirectory
        self.parsed = None
        self.project = None


def _unpack(ctx):
    """Unpacks message content.

    """
    ctx.cpt = base64.decodestring(ctx.content['data'])
    ctx.centre = ctx.content['centre'].lower()


def _validate(ctx):
    """Validates incoming cpt file.

    """
    # CPT file is empty.
    if not ctx.cpt:
        logger.log_mq_warning("CONSO: empty cpt file -> {}".format(ctx.centre))
        ctx.abort = True


def _parse(ctx):
    """Parses incoming cpt file.

    """
    # Set parsed CPT file (i.e. a dictionary)
    ctx.parsed = hcpt.parse(ctx.centre, ctx.cpt)
    if not ctx.parsed:
        ctx.abort = True
        return

    # Set date.
    ctx.date = ctx.parsed[0]['project_consumption'][0]['machine_allocation_date']


def _write(ctx):
    """Writes cpt files to file system.

    """
    # Set details of files to be written.
    files = [('raw', 'txt', ctx.cpt)] + \
            [('json', 'json', convert.dict_to_json(i)) for i in ctx.parsed]

    # Iterate file information & write to fs.
    for fdir, fsuffix, fcontent in files:
        # Set output directory.
        odir = ctx.output_dir
        odir = os.path.join(odir, fdir)
        odir = os.path.join(odir, ctx.centre)
        odir = os.path.join(odir, ctx.date)
        if not os.path.exists(odir):
            os.makedirs(odir)

        # Set output file name.
        fhash = hashlib.md5(fcontent).hexdigest()
        fname = "{}.{}".format(fhash, fsuffix)

        # Set output file path.
        fpath = os.path.join(odir, fname)

        # Skip if file already has been processed.
        if os.path.exists(fpath):
            logger.log_mq_warning("CONSO - skipping duplicate {} file".format(fpath))
            continue

        # Write to file system.
        with open(fpath, 'w') as fstream:
            fstream.write(fcontent)
