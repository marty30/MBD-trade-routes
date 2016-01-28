# Template Spark Project for the CTIT Cluster

This template contains some settings and examples in Java and Scala.

## Setup
To run commands in the background (/usr/lib/spark/bin/spark) or in a shell (/usr/lib/spark/bin/spark-shell), you have to call supply several parameters. The file `setenv` stores these parameters. Please adapt this file before you continue.

To compile the project do the following
```
mvn package
```

## Prepare for running jobs

For each session, you first have to load the environment file `setenv`:
```
. setenv
````

## Running a background job
You can now run a background job by using the command `runTool`. For example, to run the included `JavaWordCount` example, use:
```
runTool JavaWordCount input output
```
where input and output the source and target directories.

Alternatively, you can also run the equivalent job in Scala:
```
runTool ScalaWordCount input output
```

## Running an interactive shell
You can also run an interactive shell, for this you type:
```
runShell
```
There will be some status messages and then you arrive at a prompt where you can insert scala commands interactively, for example:
```
val text = sc.textFile("input")
val map  = text.flatMap(line => line.split(" ")).map(word => (word,1))
val reduce = map.reduceByKey((a,b) => a + b)
reduce.saveAsTextFile("output")
```

If you want to process an existing script, you can also do:
```
cat script | runShell
```
