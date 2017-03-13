import uuid

from prodiguer import mq
from prodiguer import __version__ as HERMES_VERSION


def _get_msg_props():
    """Returns AMPQ message properties.

    """
    return mq.utils.create_ampq_message_properties(
        user_id=mq.constants.USER_HERMES,
        producer_id=mq.constants.PRODUCER_IGCM,
        producer_version="2.8.2",
        message_type="8888"
        )


def _get_msg_payload():
    """Returns AMPQ message payload.

    """
    return {
    	"simuid": unicode(uuid.uuid4())
    }


def _yield_message():
    """Yeild a mesage to be enqueued.

    """
    print _get_msg_props()
    print _get_msg_payload()
    yield mq.Message(_get_msg_props(), _get_msg_payload())


mq.produce(_yield_message)
