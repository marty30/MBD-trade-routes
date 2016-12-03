#!/bin/bash
TOOL=$1
shift
TOOLTEST="${TOOL}Test"
echo $TOOL >> src/main/resources/tools.txt
sed -e "s/object Template/object $TOOL/g" src/main/scala/nl/utwente/bigdata/Template.scala > src/main/java/nl/utwente/bigdata/$TOOL.scala
