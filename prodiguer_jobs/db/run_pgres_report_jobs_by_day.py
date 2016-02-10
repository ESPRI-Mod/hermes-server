import argparse
import datetime
import json
import os
import sys

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring




# Define command line arguments.
_parser = argparse.ArgumentParser("Generates accounting project related report.")
_parser.add_argument(
    "-dest", "--dest",
    help="Directory intow hich report will be written",
    dest="dest",
    type=str
    )
_parser.add_argument(
    "-i", "--interval",
    help="Number of days for which to generate report",
    dest="days",
    type=int,
    default=14
    )


def _get_accounting_projects():
    """Gets set of accouting projects from db.

    """
    with db.session.create():
        return [{
            "name": ap,
            "counts": [],
            "max": None,
            "min": None,
            "avg": None
        } for ap in dao_monitoring.get_accounting_projects()]


def _get_intervals(days):
    """Gets set of time intervals over which to query jobs.

    """
    today = datetime.datetime.now().date()
    today = datetime.datetime(today.year, today.month, today.day)

    return [(today + datetime.timedelta(days=0 - i),
             today + datetime.timedelta(days=1 - i)) for i in range(days + 1)[1:]]


def _get_job_set(start, end):
    """Returns a set of jobs for a time interval.

    """
    with db.session.create():
        return dao_monitoring.retrieve_jobs_by_interval(start, end)


def _write_report(stats, dest):
    """Writes stats to file system.

    """
    fpath = os.path.join(dest, "prodiguer-report-jobs-by-day.json")
    with open(fpath, 'w') as f:
        f.write(json.dumps(stats, indent=4))


def _main(args):
    """Main entry point.

    """
    # Initialise stats.
    stats = _get_accounting_projects()

    # Set job counts.
    for start, end in _get_intervals(args.days):
        job_set = _get_job_set(start, end)
        for ap in stats:
            ap['counts'].append(len([j for j in job_set if j.accounting_project == ap['name']]))

    # Set derived stats.
    for ap in stats:
        ap['min'] = min(ap['counts'])
        ap['max'] = max(ap['counts'])
        ap['avg'] = sum(ap['counts']) / len(ap['counts'])

    # Write report to file system.
    _write_report(stats, args.dest)


# Main entry point.
if __name__ == '__main__':
    _main(_parser.parse_args())
