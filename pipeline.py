import luigi
import psycopg2 as pg
from luigi.contrib.spark import SparkSubmitTask
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class PSQLConn(object):
    """Stores the connection to psql."""
    def __init__(self, db, user, password, host):
        self.db = db
        self.user = user
        self.password = password
        self.host = host

    def connect(self):
        connection = pg.connect(
                host=self.host,
                database=self.db,
                user=self.user,
                password=self.password)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        return connection

# Assign credentials here
cred = PSQLConn("dbname", "user", "password", "host")


class PostgresTask(luigi.Task):
    """Create a table and then check if this table exits."""
    def output(self):
        return luigi.LocalTarget("target/task1_dummy.csv")

    def run(self):
        conn = cred.connect()
        with conn.cursor() as curs:
            curs.execute("CREATE dummy_table;")

        with conn.cursor() as curs:
            curs.execute("SELECT * FROM dummy_table;")
            rows = curs.fetchall()

        with self.output().open("w") as out_file:
            for row in rows:
                out_file.write(str(row))


class SparkTask(SparkSubmitTask):
    """Run a spark script only if PostgresTask finished before."""
    name = "Simple Count"
    app = "spark_app.py"

    def output(self):
        return luigi.LocalTarget("target/task2_dummy.csv")

    def requires(self):
        return PostgresTask()


if __name__ == "__main__":
    # Use local scheduler for development purposes.
    luigi.run(["--local-scheduler"], main_task_cls=SparkTask)
