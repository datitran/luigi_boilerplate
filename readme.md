## Luigi Boilerplate for Postgres + PySpark

This is a simple boilerplate for setting up a pipeline for Postgres and PySpark with [Luigi](https://github.com/spotify/luigi). It includes only a dummy postgres task and a pyspark task. The boilerplate is work-in-progress.

Run `python pipeline.py` to start the workflow. I included a dummy target for the Postgres task since the user has to provide the connection to the database in the `PSQLConn` class. In practice, unlike GNU Make, Luigi does not remove the target but it has to be removed by the user. A dummy target will be created for the Spark task which has to be removed as well when you want to restart the pipeline.

Author: Dat Tran
