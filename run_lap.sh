/home/spark/spark/bin/spark-submit \
--class org.ics.isl.LAP \
--driver-memory 10G \
--executor-memory 210G \
--conf spark.speculation=true \
--master mesos://zk://clusternode1:2181,clusternode2:2181,clusternode3:2181/mesos \
/home/jagathan/LAWA/target/LAWA-1.0-SNAPSHOT.jar $1 $2
