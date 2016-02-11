# -*- coding: utf-8 -*-

"""
.. module:: run_pgres_report_jobs_by_day.py
   :copyright: Copyright "Feb 10, 2016", Institute Pierre Simon Laplace
   :license: GPL/CeCIL
   :platform: Unix
   :synopsis: Generates a database report of the number of jobs by day by accounting project.

.. moduleauthor:: Mark Conway-Greenslade <momipsl@ipsl.jussieu.fr>


"""
import argparse
import datetime
import os

from prodiguer.db import pgres as db
from prodiguer.db.pgres import dao_monitoring




# Define command line arguments.
_parser = argparse.ArgumentParser("Generates accounting project related report.")
_parser.add_argument(
    "-dest", "--dest",
    help="Directory into which report will be written",
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


def _get_initial_stats():
    """Gets initial stats prior to querying jobs table.

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
    with db.session.create():
        earliest_job = dao_monitoring.get_earliest_job()

    start = earliest_job.execution_start_date.date()
    start = datetime.datetime(start.year, start.month, start.day)

    end = datetime.datetime.now() - datetime.timedelta(days=1)
    end = datetime.datetime(end.year, end.month, end.day)

    return [(start + datetime.timedelta(days=0 + i),
             start + datetime.timedelta(days=1 + i))
            for i in range((end - start).days)]


def _get_job_set(start, end):
    """Returns a set of jobs for a time interval.

    """
    with db.session.create():
        return dao_monitoring.retrieve_jobs_by_interval(start, end)


def _write_report(stats, dest):
    """Writes stats to file system.

    """
    def _format_line(f1, f2, f3, f4, f5):
        """Returns a formatted line.

        """
        return "{}\t{}\t{}\t{}\t{}\n".format(
            f1.rjust(15), f2.rjust(5), f3.rjust(5), f4.rjust(5), f5)

    # Transform stats into report lines.
    lines = [_format_line("Acc. Project", "Min", "Max", "Avg", "Time Series"), "\n"]
    for s in sorted(stats, key=lambda s: s['name']):
        lines.append(_format_line(s['name'], repr(s['min']), repr(s['max']), repr(s['avg']), s['counts']))

    # Write report to file system.
    fpath = os.path.join(dest, "prodiguer-report-jobs-by-day.txt")
    with open(fpath, 'w') as f:
        f.writelines(lines)


def _main(args):
    """Main entry point.

    """
    # Initialise stats.
    stats = _get_initial_stats()

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
    # Validate args.
    args = _parser.parse_args()
    if not os.path.exists(args.dest):
        raise ValueError("Report output directory is invalid.")

    # Invoke entry point.
    _main(args)
