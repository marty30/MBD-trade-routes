#!/bin/bash
TOOL=$1
shift
sed -e "s/object Template/object $TOOL/g" src/main/scala/nl/utwente/bigdata/Template.scala > src/main/scala/nl/utwente/bigdata/$TOOL.scala
echo "Created template for $TOOL, you can now edit the file: src/main/scala/nl/utwente/bigdata/$TOOL.scala"
