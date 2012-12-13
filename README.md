GraphiteContext
===============

Like the GangliaContext for Hadoop, sends metrics to Graphite

Requirements
------------
maven

Compilation
-----------

    $ mvn clean package

Installation
------------

In your hadoop-env.sh file (usually in /etc/hadoop/conf/), add the
location of the graphite-context-*.jar file to HADOOP_CLASSPATH

    export HADOOP_CLASSPATH="~/GraphiteContext/target/graphite-context-1.0.0-SNAPSHOT.jar"

Alternatively, drop the jar into the default hadoop libraries.

Configuration
-------------

In your hadoop-metrics.properties file, add the following for all
metrics you want forwarded to Graphite

    @metric@.class=org.apache.hadoop.metrics.graphite.GraphiteContext
    @metric@.period=60
    @metric@.serverName=@Your Graphite Server@
    @metric@.port=@Your Graphite Server port@
    @metric@.path=@Graphite prefix path@

`@metric@.path` defaults to Platform.Hadoop

Restart the required daemons, for instance:

    $sudo service hadoop-tasktracker restart
