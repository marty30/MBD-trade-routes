# Cluster Access

To access the Hadoop Cluster of the University of Twente, called CTIT cluster, you have to follow the following steps:

<img style="height: 400px" src="cluster.svg" alt="Cluster architecture">


## Prerequisites

* You should have a username (referred to as <username>) and password for the cluster (if you don't, apply for it at the system administrator and change the password immediately [here](https://tap.utwente.nl/tap/)).
* This document assumes knowledge about the handline of the following topics:
 * The hadoop file system, a tutorial can be found [here](https://www.tutorialspoint.com/hadoop/hadoop_hdfs_operations.htm).
 * Unix/Linux command line, tutorials can be for example found [here](http://www.ee.surrey.ac.uk/Teaching/Unix/) and [here](http://linuxcommand.org/learning_the_shell.php).
 * Programming with the build system maven, tutorials can be found [here](https://maven.apache.org/guides/getting-started/maven-in-five-minutes.html)
* If you work from your own computer, you should also have:
 * an SSH client, and 
 * an SCP/RSYNC client 

For links of recommended software, see the software section below.

## Connecting to cluster

* SSH to the so-called CTIT head nodes, which serve as entry points to a cluster, i.e. **ctithead1.ewi.utwente.nl** and **ctithead2.ewi.utwente.nl**: ``ssh <username>@ctithead1.ewi.utwente.nl``
* Should you be using public key authentification (by default you do not), you have to add an extra authentication step: ``kinit <username>`` 

Connection test:

    hdfs dfs -ls 
    yarn application -list

## Create Code

Depending on your preferences, you can create code on the ctithead nodes or on your local computer. The following instructions
can be applied for both:

* Check out the source code template directory: ``git clone https://github.com/robinaly/ctit-spark.git <projectname>``.
* Change into that directory: ``cd <projectname>``
* Create a new task: ``./createTool.sh [python|java|scala] <ToolName>``
* Edit the source file in an editor
* If you are onn your local computer, you can upload the source code to the head node, by:
 * ``rsync -avug ./ <username>@ctithead1.ewi.utwente.nl:~/<projectname>/``
 * In subsequent calls it is better to only copy the directory with sources: ``rsync -avug ./src/ <username>@ctithead1.ewi.utwente.nl:~/<projectname>/src/``

## Uploading data

If you want to upload data to the cluster follow these steps, we assume your data is in a directory called <dir>:

* Upload data to your home directory on the hosts ``ctithead1.ewi.utwente.nl``, or ``ctithead2.ewi.utwente.nl``  (e.g. via scp or rsync)
  * ``rsync -avug <dir>/ <username>@ctithead1.ewi.utwente.nl:~/<dir>/``
  * ``scp -r <dir> <username>@ctithead1.ewi.utwente.nl:~/`` 
* ssh to the the respective machine
* Optionally create a suitable parent directory for your data on the hdfs, e.g.: ``hdfs dfs -mkdir collections``
* Put a file on the cluster ``hdfs dfs -put <dir> <dir>/``

## Starting A  Job

* Connect to the cluster
* Change into the directory where your project resides
* Load required settings: ``. setenv`` 
* Compile code: ``mvn package``
* Fix potential errors
* Submit job: ``runTool <TaskName> Argument1 Argument 2...`` 
 
## Debugging

Debugging distributed systems can be cumbersome. At the moment, the best strategy is produce log output inside your code. When you run the application, note down the **applicationId**, starting with application\_, from the output. 

To see the progress of your task you can visit the website:

   [http://ctit048.ewi.utwente.nl:8088/cluster](http://ctit048.ewi.utwente.nl:8088/cluster)

Use the following command to read through logs:

    yarn logs --applicationId <applicationId>    
    
If you want to kill an application, you can use:

    yarn application --kill <applicationId>


## Downloading data

To download your data to your local computer follow these steps, we assume that the data is in directory <dir>:

* Connect to the cluster (make sure that you are in the root directory)
* Download the data from hdfs to your home directory: ``hdfs dfs -get <dir>``
* Disconnect from the cluster
* Copy the data to your computer:
 * Change into the directory where the data should be stored
 * Copy the data by one of the following:
  * ``rsync -avug <username>@ctithead1.ewi.utwente.nl:~/<dir> ./``
  * ``scp <username>@ctithead1.ewi.utwente.nl:~/<dir> ./``
 * Now you can further analyze the data.

## Software / Links

### Connecting from Windows
* Remote access: [Putty](http://www.chiark.greenend.org.uk/~sgtatham/putty/download.html)
* Copy tool for indvidual files: [WinSCP](https://winscp.net/eng/download.php#download2)
* Copy tool for directory trees: [DeltaCopy](http://www.aboutmyip.com/AboutMyXApp/DeltaCopy.jsp)

### Coding
* Text Editor: [Sublime](https://www.sublimetext.com/)
* Versioning: [Windows Git GUIs](https://git-scm.com/downloads/guis)
* Versioning: [Mac Git GUI](https://www.sourcetreeapp.com/)

### Software repositories
* Spark templates: [Ctit-Spark](https://github.com/robinaly/ctit-spark)
* Hadoop map reduce templates: [Ctit-mapred](https://github.com/robinaly/ctit-mapred)


