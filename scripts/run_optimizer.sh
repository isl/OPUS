spark-submit \
--class org.ics.isl.QueryOptimizer \
--driver-memory DRIVER_MEM \
--executor-memory EXECUTOR_MEM \
--conf spark.speculation=true \
--master MASTER \
PATH_TO_JAR $1 $2 $3 $4
