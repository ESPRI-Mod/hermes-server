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


class _ProcessingContextInfo(object):
    """Encapsulates information used during processing.

    """
    def __init__(self, io_dir):
        if not os.path.exists(io_dir):
            raise ValueError("Report output directory is invalid.")

        self.mail_to_sim_map = {}
        self.sim_to_ap_map = {}
        self.stats = {}
        self.io_dir = io_dir


def _init_maps(ctx):
    """Initialises maps that simplify processing.

    """
    ctx.sim_to_ap_map = {i[0]: i[1] for i in dao_monitoring.get_simulation_accounting_projects()}
    ctx.mail_to_sim_map = {i[0]: i[1] for i in dao_mq.retrieve_mail_simulation_identifiers() if i[1] in ctx.sim_to_ap_map}


def _init_stats(ctx):
    """Initialises set of stats to be emitted.

    """
    ctx.stats = [{
        "name": ap,
        "counts": [],
        "max": None,
        "min": None,
        "avg": None
    } for ap in dao_monitoring.get_accounting_projects()]


def _init_intervals(ctx):
    """Initialises time intervals for which stats will be generated.

    """
    # Start from arrival date of earliest email.
    earliest_mail = dao_mq.get_earliest_mail()
    start = earliest_mail.arrival_date.date()
    start = datetime.datetime(start.year, start.month, start.day)

    # End yesterday.
    end = datetime.datetime.now() - datetime.timedelta(days=1)
    end = datetime.datetime(end.year, end.month, end.day)

    # Set hourly timeslices.
    ctx.intervals = [(start + datetime.timedelta(hours=0 + i),
                      start + datetime.timedelta(hours=1 + i))
                      for i in range(((end - start).days * 24))]


def _get_interval_stats(ctx, start, end):
    """Returns statistics for a single interval.

    """
    # Get interval email set.
    with db.session.create():
        data = dao_mq.retrieve_mail_identifiers_by_interval(start, end)

    # Exclude those not mapped to a simulation.
    data = [i for i in data if i in ctx.mail_to_sim_map]

    # Map emails to accounting projects.
    data = [(i, ctx.sim_to_ap_map[ctx.mail_to_sim_map[i]]) for i in data]

    # Exclude those not mapped to an accounting project.
    data = [i for i in data if i[1] is not None]

    return data


def _set_interval_stats(ctx):
    """Sets stats for a timeslice.

    """
    for start, end in ctx.intervals:
        interval_stats = _get_interval_stats(ctx, start, end)
        for ap in ctx.stats:
            ap['counts'].append(len([s for s in interval_stats if s[1] == ap['name']]))


def _set_summary_stats(ctx):
    """Set summary statistics.

    """
    for ap in ctx.stats:
        ap['min'] = min(ap['counts'])
        ap['max'] = max(ap['counts'])
        ap['avg'] = sum(ap['counts']) / float(len(ap['counts']))


def _get_report_header(ctx):
    """Returns report header.

    """
    start = ctx.intervals[0][0]
    end = ctx.intervals[-1][1]

    header = []
    header.append("Report Title:     Summary of incoming mails per hour per accounting project\n")
    header.append("Report Date:      {}\n".format(datetime.datetime.now().date()))
    header.append("Report Interval:  {} - {} ({} days)\n".format(start.date(), end.date(), (end - start).days))
    header.append("\n")
    header.append("{:>15}{:>10}{:>10}{:>10}\n".format("Acc. Project", "Min", "Max", "Avg"))
    header.append("\n")

    return header


def _get_report_body(ctx):
    """Returns report body.

    """
    body = []
    for s in sorted(ctx.stats, key=lambda s: s['name']):
        body.append("{:>15}{:>10}{:>10}{:>10.2f}\n".format(s['name'], s['min'], s['max'], s['avg']))

    return body


def _write_report(ctx):
    """Writes stats to file system.

    """
    fpath = os.path.join(ctx.io_dir, "mails-per-hour-summary.txt")
    with open(fpath, 'w') as f:
        f.writelines(_get_report_header(ctx) + _get_report_body(ctx))


def _main(args):
    """Main entry point.

    """
    ctx = _ProcessingContextInfo(args.dest)

    # Pull as much data from db as possibile upfront.
    with db.session.create():
        _init_maps(ctx)
        _init_stats(ctx)
        _init_intervals(ctx)

    # Set interval stats.
    _set_interval_stats(ctx)

    # Set summary stats.
    _set_summary_stats(ctx)

    # Write report.
    _write_report(ctx)


# Main entry point.
if __name__ == '__main__':
    # Invoke entry point.
    _main(_parser.parse_args())
