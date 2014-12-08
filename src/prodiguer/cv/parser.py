from ..db import cache
from ..db.types import CvTerm


def parse(term_type, term_name):
    """Parses a controlled vocabulary term.

    :param str term_type: Type of CV term being parsed, e.g. activity.
    :param str term_name: Name of CV term being parsed, e.g. ipsl.

    :returns: Parsed cv term name.
    :rtype: unicode

    """
    # Format inputs.
    term_type = unicode(term_type).lower()
    term_name = unicode(term_name).lower()

    # Set filtered terms.
    terms = cache.get_collection(CvTerm)
    terms = [i for i in terms if i.cv_type == term_type]

    # Match term by name or synonym.
    for term in terms:
        # ... name match.
        if term.name.lower() == term_name:
            return term.name
        # ... synonym match.
        if term.synonyms:
            names = term.synonyms.split(",")
            names = [n.strip().lower() for n in names if n and n.strip()]
            if term_name in names:
                return term.name

    # Term was unmatched therefore error.
    err = "Unknown cv term: {0}.{1}".format(term_type, term_name)
    raise ValueError(err)
