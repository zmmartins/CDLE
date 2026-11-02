"""A small Spark for the tests: local, inside the client container, without the cluster (it starts in a few seconds)."""
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("tests")
         .config("spark.eventLog.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
    yield s
    s.stop()


@pytest.fixture
def lines(spark):
    """lines(["one log line", "another"]) -> the DataFrame that spark.read.text would give for those lines."""
    def make(texts):
        return spark.createDataFrame([(t,) for t in texts], "value string")
    return make
