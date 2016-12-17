import $toolName
import pytest
import pyspark

# create necessary test framework
@pytest.fixture(scope="session",
                params=[pytest.mark.spark_local('local'),
                        pytest.mark.spark_yarn('yarn')])
def spark_context(request):
    if request.param == 'local':
        conf = (pyspark.SparkConf()
                .setMaster("local[2]")
                .setAppName("pytest-pyspark-local-testing")
                )
    elif request.param == 'yarn':
        conf = (pyspark.SparkConf()
                .setMaster("yarn-client")
                .setAppName("pytest-pyspark-yarn-testing")
                .set("spark.executor.memory", "1g")
                .set("spark.executor.instances", 2)
                )
    request.addfinalizer(lambda: sc.stop())

    sc = pyspark.SparkContext(conf=conf)
    return sc

# add test cases
def test_that_requires_sc(spark_context):
  assert true
  #assert PythonWordCount.doJob(spark_context.parallelize(["12123 123123","3423"])).count() == 3