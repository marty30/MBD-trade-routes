# Template Spark Project for the CTIT Cluster

This template contains some settings and examples in Python, Java, and Scala.

## Creating a new Job

To create a new job, use the provided ``createTool.sh``, which is a script that create an empty job 
from a suitable template located in the folder ``templates``. The script is called as follows:

    python ./createTool.sh <language> <toolname>

, where

* <langauge> is scala, java or python, and
* <toolname> is the name of the tool (by convention starting with an upper case).
  
## Compilation

If you have Java or Scala jobs, you can compile them with the command as follows

    mvn package
    
## Testing

Before you run a job on the cluster, please test the code locally. To do this, 
you have to define so-called test cases. All tests assume that you have the environment variable SPARK_HOME 
pointing to the top directory of your spark installation, which can be downloaded [here](http://spark.apache.org/downloads.html). 


### Scala

If you created a tool with ``createTool.sh``, your test cases should be stored in the file
``src/test/scala/nl/utwente/bigdat/<ToolName>Test.scala``. In the file there is a commented-out
test case.

### Python

Test cases for python jobs are stored in the same directory as the
job itself: ``src/main/python``.  The tests require the package
``pytest``, which can be installed with the command 

    pip install pytest

. To execute the tests, you first have to include the python
adaptor for spark and the library py4j into your python path:

    export PYTHONPATH=$PYTHONPATH:$SPARK_HOME/python/:$SPARK_HOME/python/lib/py4j-*-src.zip
    
And to actually execute the tests:

    py.test src/main/python/<ToolName>Test.py

## Access to the cluster

The documentation of how to access the cluster can be found [here](access.md).

## Datasets

The documentation of available datasets on the cluster can be found [here](data.md).

## Creating html versions of the documentation

To create a self-contained html version of the documentation use the following commands (requires pandocs)

    pandoc -T "Cluster Access" --toc -f markdown_github access.md -N -t html -o access.html
    encodeImages.py access.html

