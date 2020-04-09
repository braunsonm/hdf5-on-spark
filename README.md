# hdf5-on-spark

This repository is the source code of a paper I wrote which detailed some performance improvements for the P2IRC Project and their storage of HDF5 files. This implementation provides a HDF5 reader which ingests our HDF5 files into a binary format for storage on Hadoop HDFS. We also make use of Elasticsearch for storing metadata about the partitions and blocks for faster filtering and RDD reconstruction.

This implementation is very specific to our use case and setup. However, it is open source so that other developers may learn from it when looking for a solution to doing HDF5 processing on a Spark/Hadoop (or another storage backend) cluster.

It's also possible to write some Python APIs to make job submission easier for data scientists.

Feel free to open an issue if you have any questions!

## Development Setup

In order to get setup for development there is a few things you need to install first.

- Scala 2.11
- Java 8
- Hadoop 2.7.7
- Spark 2.4.1

Then you can get your Hadoop cluster setup by doing the following:

- Download and extract the precompiled hadoop archive
- Ensure you have SSHd running without passwords on your local machine (ie: setup SSH keys)
- Set the `JAVA_HOME` variable in `etc/hadoop/hadoop-env.sh`
`export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_121.jdk/Contents/Home`
- Configure:

`etc/hadoop/core-site.xml`

```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
```

`etc/hadoop/hdfs-site.xml`

```xml
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>
    <!-- Optionally change the data directory -->
    <property>
        <name>dfs.name.dir</name>
        <value>/usr/local/hadoop/dfs/name</value>
        <final>true</final>
    </property>
</configuration>
```

- Initialize the Hadoop Cluster by running `bin/hdfs namenode -format`
- Note, a great guide for MacOS is here: https://www.quickprogrammingtips.com/big-data/how-to-install-hadoop-on-mac-os-x-el-capitan.html

Then you can setup your Spark cluster by doing the following:

- For spark, it's easy. Just download the prebuilt package, extract it.
- Edit some of the configuration files to your liking, for instance:

`conf/spark-defaults.conf`:
```shell script
# Example:
# spark.master                     spark://master:7077
# spark.eventLog.enabled           true
# spark.eventLog.dir               hdfs://namenode:8021/directory
# spark.serializer                 org.apache.spark.serializer.KryoSerializer
spark.driver.memory                1g
spark.executor.memory		   4g
spark.driver.cores		   1
spark.executor.cores		   2
spark.driver.extraJavaOptions	-Djava.library.path=/path/to/this/repo/lib
spark.executor.extraJavaOptions	-Djava.library.path=/path/to/this/repo/lib
```

The java library path is of particular instance to link to this repositories lib/ folder when running locally if you aren't using an assembled jar to submit jobs with. Otherwise it can be safely ignored.

`conf/spark-env.sh`:
If you have multiple java versions installed you may want to provide a `JAVA_HOME` to the driver and executors.

```
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk1.8.0_202.jdk/Contents/Home
```

Lastly, you can start/stop your local cluster by using the following commands.

- Startup:
```shell script
# In Hadoop Install Directory
sbin/start-dfs.sh
# WebUI Running on http://localhost:50070

# In Spark Install Directory
sbin/start-all.sh
# WebUI Running on http://localhost:8080
```

- Shutdown:
```shell script
# In Hadoop Install Directory
sbin/stop-dfs.sh

# In Spark Install Directory
sbin/stop-all.sh
```

To submit jobs to a running cluster you can use the included `scripts/ingest.sh`, but first
make sure to copy the `.env.template` to `.env` and edit it accordingly.

## Some benchmark commands

- Python Naive Filter
```
# Combined keith data
 python3 src/main/python/naiveFilter.py /trux/data/P2IRC/hdf5/keith --time-gte 1533833720014 --time-lte 1533837525405
# Small data
 python3 src/main/python/naiveFilter.py /trux/data/P2IRC/hdf5/small-data --time-gte 1527207282644 --time-lte 1527238446092
```

## Libraries

We make use of two HDF5 Libraries, jhdf5 and hdf5-java. Everything in lib except jarhdf5 is jhdf5 and needs to be there.

We include the batteries included jar and the jarhdf5 jar.