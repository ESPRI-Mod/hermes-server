# -*- coding: utf-8 -*-

"""
.. module:: pcmdi.py
   :copyright: Copyright "Mar 21, 2015", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Consumes monitoring 7100 messages (PCMDI metrics).

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import base64
import os
import shutil
import tempfile
import uuid

import hermes_client
from prodiguer import mq



# Actions to take when uploading duplicate metrics.
_ADD_DUPLICATE_ACTION_SKIP = 'skip'

# Default metric group identifier.
_DEFAULT_METRIC_GROUP_ID = 'XXXXXX'



def get_tasks():
    """Returns set of tasks to be executed when processing a message.

    """
    return (
      _unpack_content,
      _decode_metrics,
      _save_metrics_to_file_system,
      _format_metrics,
      _upload_metrics,
      _cleanup
      )


class ProcessingContextInfo(mq.Message):
    """Message processing context information.

    """
    def __init__(self, props, body, decode=True):
        """Object constructor.

        """
        super(ProcessingContextInfo, self).__init__(
            props, body, decode=decode)

        self.metrics_base64 = None
        self.metrics_json = None
        self.metrics_fpath = "{}.json".format(uuid.uuid4())
        self.metrics_group_id = None
        self.dir_raw = None
        self.dir_formatted = None


def _unpack_content(ctx):
    """Unpacks message being processed.

    """
    ctx.metrics_base64 = ctx.content['metrics']
    ctx.metrics_group_id = ctx.content.get('metricsGroupName', _DEFAULT_METRIC_GROUP_ID)


def _decode_metrics(ctx):
    """Decodes base64 encoded metrics.

    """
    ctx.metrics_json = base64.decodestring(ctx.metrics_base64)


def _save_metrics_to_file_system(ctx):
    """Saves decoded metrics to file system in readiness for upload.

    """
    ctx.dir_formatted = tempfile.mkdtemp()
    ctx.dir_raw = tempfile.mkdtemp()
    ctx.metrics_fpath = os.path.join(ctx.dir_raw, ctx.metrics_fpath)
    with open(ctx.metrics_fpath, 'w') as out_file:
        out_file.write(ctx.metrics_json)


def _format_metrics(ctx):
    """Formats metrics in readiness for upload.

    """
    hermes_client.metrics.format(ctx.metrics_group_id, ctx.dir_raw, ctx.dir_formatted)


def _upload_metrics(ctx):
    """Uploads metrics to API.

    """
    hermes_client.metrics.add_batch(ctx.dir_formatted, _ADD_DUPLICATE_ACTION_SKIP)


def _cleanup(ctx):
    """Performs post processing clean up operations.

    """
    if ctx.dir_formatted:
        shutil.rmtree(ctx.dir_formatted)
    if ctx.dir_raw:
        shutil.rmtree(ctx.dir_raw)
