
import arrow
import psycopg2

from prodiguer.db import pgres as db
from prodiguer.utils import config



class TestMetrics(object):
	"""Encapsulates a set of test metrics.

	"""
	def __init__(self, level, desc, target):
		self.data = None
		self.desc = desc.upper()
		self.level = level.upper()
		self.target = target.upper()
		self.time = None
		self._then = None

	def __repr__(self):
		return "{}, {}, {}, {}, {}".format(self.time, self.level, self.target, self.desc, len(self.data))


	def start(self):
		self._then = arrow.now()

	def end(self):
		self.time = arrow.now() - self._then


class TestContextInformation(object):
	"""Encapsulates testing contextual information.

	"""
	def __init__(self):
		self.hl_01_sims = TestMetrics("high", "all", "sims")
		self.hl_01_jobs = TestMetrics("high", "all", "jobs")
		self.hl_02_sims = TestMetrics("high", "active timeslice", "sims")
		self.hl_02_jobs = TestMetrics("high", "active timeslice", "jobs")
		self.ll_01_sims = TestMetrics("low", "all", "sims")
		self.ll_01_jobs = TestMetrics("low", "all", "jobs")
		self.ll_02_sims = TestMetrics("low", "active timeslice", "sims")
		self.ll_02_jobs = TestMetrics("low", "active timeslice", "jobs")
		self.tests = [
			self.hl_01_sims,
			self.hl_01_jobs,
			self.hl_02_sims,
			self.hl_02_jobs,
			self.ll_01_sims,
			self.ll_01_jobs,
			self.ll_02_sims,
			self.ll_02_jobs,
		]


def get_psycopg2_connection():
	"""Returns a low-level connection to remote database.

	"""
	try:
	    return psycopg2.connect(config.db.pgres.main)
	except:
		raise RuntimeError("unable to connect to the database")


def _do_test_hl_01(ctx):
	db.session.start()

	ctx.hl_01_sims.start()
	ctx.hl_01_sims.data = db.dao.get_all(db.types.Simulation)
	ctx.hl_01_sims.end()

	ctx.hl_01_jobs.start()
	ctx.hl_01_jobs.data = db.dao.get_all(db.types.Job)
	ctx.hl_01_jobs.end()

	db.session.end()


def _do_test_hl_02(ctx):
	db.session.start()

	ctx.hl_02_sims.start()
	ctx.hl_02_sims.data = db.dao_monitoring.retrieve_active_simulations()
	ctx.hl_02_sims.end()

	ctx.hl_02_jobs.start()
	ctx.hl_02_jobs.data = db.dao_monitoring.retrieve_active_jobs()
	ctx.hl_02_jobs.end()

	db.session.end()


def _do_test_ll_01(ctx):
	conn = get_psycopg2_connection()
	cur = conn.cursor()

	ctx.ll_01_sims.start()
	cur.execute("""SELECT * from monitoring.tbl_simulation""")
	ctx.ll_01_sims.data = cur.fetchall()
	ctx.ll_01_sims.end()

	ctx.ll_01_jobs.start()
	cur.execute("""SELECT * from monitoring.tbl_job""")
	ctx.ll_01_jobs.data = cur.fetchall()
	ctx.ll_01_jobs.end()

	conn.close()


def _do_test_ll_02(ctx):
	conn = get_psycopg2_connection()
	cur = conn.cursor()

	ctx.ll_02_sims.start()
	cur.execute("""	SELECT
						s.*
					FROM
						monitoring.tbl_simulation as s
				""")
	ctx.ll_02_sims.data = cur.fetchall()
	ctx.ll_02_sims.end()

	ctx.ll_02_jobs.start()
	cur.execute("""	SELECT
					    j.simulation_uid,
					    j.job_uid,
					    j.accounting_project,
					    j.execution_end_date,
					    j.execution_start_date,
						j.id,
					    j.is_error,
					    j.typeof,
					    j.warning_delay,
					    j.is_startup,
					    j.post_processing_name,
					    j.post_processing_date,
					    j.post_processing_dimension,
					    j.post_processing_component,
					    j.post_processing_file,
					    j.row_create_date,
					    j.row_update_date
				   	FROM
				   		monitoring.tbl_job as j
		           	JOIN
		           		monitoring.tbl_simulation ON j.simulation_uid = monitoring.tbl_simulation.uid
		           	WHERE
		           		monitoring.tbl_simulation.execution_start_date IS NOT NULL AND
		           		monitoring.tbl_simulation.is_obsolete = false
		        """)
	ctx.ll_02_jobs.data = cur.fetchall()
	ctx.ll_02_jobs.end()

	conn.close()

# Run tests.
ctx = TestContextInformation()
for func in [_do_test_hl_01, _do_test_hl_02, _do_test_ll_01, _do_test_ll_02]:
	func(ctx)


for t in ctx.tests:
	print t


