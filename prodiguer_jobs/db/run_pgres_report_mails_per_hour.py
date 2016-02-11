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
from prodiguer.db.pgres import dao_mq



# Define command line arguments.
_parser = argparse.ArgumentParser("Generates accounting project related report.")
_parser.add_argument(
    "-dest", "--dest",
    help="Directory into which report will be written",
    dest="dest",
    type=str
    )


# Map of simulation identifiers to accounting projects.
_SIMULATION_ACCOUNTING_PROJECT_MAP = {}


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


def _get_intervals():
    """Gets set of time intervals over which to query jobs.

    """
    # Start from arrival date of earliest email.
    with db.session.create():
        earliest_mail = dao_mq.get_earliest_mail()
    start = earliest_mail.arrival_date.date()
    start = datetime.datetime(start.year, start.month, start.day)

    # End yesterday.
    end = datetime.datetime.now() - datetime.timedelta(days=1)
    end = datetime.datetime(end.year, end.month, end.day)

    # Return hourly segments.
    return [(start + datetime.timedelta(hours=0 + i),
             start + datetime.timedelta(hours=1 + i))
            for i in range(((end - start).days * 24))]


def _get_report_header(start, end):
    """Returns report header.

    """
    header = []
    header.append("Report Title:     Summary of incoming mails per hour per accounting project\n")
    header.append("Report Date:      {}\n".format(datetime.datetime.now().date()))
    header.append("Report Interval:  {} - {} ({} days)\n".format(start.date(), end.date(), (end - start).days))
    header.append("\n")
    header.append("{:>15}{:>10}{:>10}{:>10}\n".format("Acc. Project", "Min", "Max", "Avg"))
    header.append("\n")

    return header


def _get_report_body(stats):
    """Returns report body.

    """
    body = []
    for s in sorted(stats, key=lambda s: s['name']):
        body.append("{:>15}{:>10}{:>10}{:>10.2f}\n".format(s['name'], s['min'], s['max'], s['avg']))

    return body


def _write_report(stats, start, end, dest):
    """Writes stats to file system.

    """
    fpath = os.path.join(dest, "mails-per-hour-summary.txt")
    with open(fpath, 'w') as f:
        f.writelines(_get_report_header(start, end) + _get_report_body(stats))



def _get_simulation_accounting_project(uid):
    """Returns a simulation's accounting project inspecting local cache prior to hitting db if necessary.

    """
    if uid not in _SIMULATION_ACCOUNTING_PROJECT_MAP:
        _SIMULATION_ACCOUNTING_PROJECT_MAP[uid] = dao_monitoring.get_simulation_accounting_project(uid)

    return _SIMULATION_ACCOUNTING_PROJECT_MAP[uid]


def _get_interval_stats(start, end):
    """Returns statistics for a single interval.

    """
    with db.session.create():
        # Get set of emails that arrived during time interval.
        data = dao_mq.retrieve_mail_identifiers_by_interval(start, end)
        if not data:
            return []
        print "Interval email count: ", len(data)

        # Map each email to a simulation identifier.
        data = [(i, dao_mq.get_mail_simulation_uid(i)) for i in data]
        print "Interval email to simulation count: ", data
        data = [i for i in data if i[1] is not None]
        if not data:
            return []
        print "Interval email to simulation count: ", len(data)

        # Map each email to an accounting project.
        data = [(i[0], _get_simulation_accounting_project(i[1])) for i in data]
        data = [(i[0], i[1][0]) for i in data if i[1] is not None]
        if not data:
            return []

    return data


def _main(args):
    """Main entry point.

    """
    # Initialise stats.
    stats = _get_initial_stats()
    email_count = 0

    # Set interval stats.
    intervals = _get_intervals()
    for start, end in intervals:
        interval_stats = _get_interval_stats(start, end)
        email_count += len(interval_stats)
        for ap in stats:
            ap['counts'].append(len([s for s in interval_stats if s[1] == ap['name']]))
            if ap['counts'][-1] > 0:
                print start, end, ap['name'], ap['counts'][-1]

    # Set derived stats.
    for ap in stats:
        ap['min'] = min(ap['counts'])
        ap['max'] = max(ap['counts'])
        ap['avg'] = sum(ap['counts']) / float(len(ap['counts']))

    # Write report to file system.
    _write_report(stats, intervals[0][0], intervals[-1][1], args.dest)

    print "EMail cint", email_count


# Main entry point.
if __name__ == '__main__':
    # Validate args.
    args = _parser.parse_args()
    if not os.path.exists(args.dest):
        raise ValueError("Report output directory is invalid.")

    # Invoke entry point.
    _main(args)
