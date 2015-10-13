import arrow
import psycopg2

from prodiguer.db import pgres as db
from prodiguer.utils import data_convertor
from prodiguer.web.utils.payload import trim_job
from prodiguer.web.utils.payload import trim_simulation



def _do_test():
	# Load data.
	db.session.start()

	then = arrow.now()
	simulations = db.dao_monitoring.retrieve_active_simulations()
	print arrow.now() - then, len(simulations), "SIMS"

	then = arrow.now()
	jobs = db.dao_monitoring.retrieve_active_jobs()
	print arrow.now() - then, len(jobs), "JOBS"

	db.session.end()


	# Convert to dictionaries
	then = arrow.now()
	simulations_1 = [db.convertor.convert(s) for s in simulations]
	print arrow.now() - then, len(simulations_1), "CONVERTED SIMS"


	then = arrow.now()
	jobs_1 = [db.convertor.convert(j) for j in jobs]
	print arrow.now() - then, len(jobs_1), "CONVERTED JOBS"


	# Convert to dictionaries & trim
	then = arrow.now()
	simulations_2 = [trim_simulation(s) for s in simulations]
	print arrow.now() - then, len(simulations_2), "TRIMMED SIMS"


	then = arrow.now()
	jobs_2 = [trim_job(j) for j in jobs]
	print arrow.now() - then, len(jobs_2), "TRIMMED JOBS"



def _do_test_sa1():
	db.session.start()

	then = arrow.now()
	jobs = db.dao.get_all(db.types.Job)
	print arrow.now() - then, len(jobs), "LOADED JOBS WITH sa 1"

	db.session.end()


def _do_test_sa2():
	db.session.start()

	then = arrow.now()
	jobs = db.dao_monitoring.retrieve_active_jobs()
	print arrow.now() - then, len(jobs), "LOADED JOBS WITH sa 2"

	db.session.end()


def _do_test_psycopg21():
	try:
	    conn = psycopg2.connect("dbname='prodiguer' user='prodiguer_db_user' host='134.157.170.104' password='N@ture93!'")
	except:
		raise RuntimeError("unable to connect to the database")

	then = arrow.now()

	cur = conn.cursor()
	cur.execute("""SELECT * from monitoring.tbl_job""")
	jobs = cur.fetchall()

	print arrow.now() - then, len(jobs), "LOADED JOBS WITH psycopg2 1"
	print type(jobs[0])


def _do_test_psycopg22():
	try:
	    conn = psycopg2.connect("dbname='prodiguer' user='prodiguer_db_user' host='134.157.170.104' password='N@ture93!'")
	except:
		raise RuntimeError("unable to connect to the database")

	then = arrow.now()

	cur = conn.cursor()
	cur.execute("""SELECT
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
				   FROM monitoring.tbl_job as j
		           JOIN monitoring.tbl_simulation ON j.simulation_uid = monitoring.tbl_simulation.uid
		           WHERE monitoring.tbl_simulation.execution_start_date IS NOT NULL AND monitoring.tbl_simulation.is_obsolete = false""")
	jobs = cur.fetchall()

	print arrow.now() - then, len(jobs), "LOADED JOBS WITH psycopg2 2"

# _do_test_sa1()
_do_test_sa2()
# _do_test_psycopg21()
_do_test_psycopg22()
