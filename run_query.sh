/home/spark/spark/bin/spark-submit \
--class org.ics.isl.QueryProcessor \
--driver-memory 20G \
--executor-memory 200G \
--conf spark.speculation=true \
--master mesos://zk://clusternode1:2181,clusternode2:2181,clusternode3:2181/mesos \
/home/jagathan/SemanticPartitioner/target/SemanticPartitioner-1.0-SNAPSHOT.jar $1 $2 $3
