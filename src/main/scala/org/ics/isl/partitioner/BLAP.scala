package org.ics.isl

import java.net.URI
import java.io._
import org.apache.spark.Partitioner
import scala.concurrent.duration._
import org.apache.spark.broadcast._
import org.apache.spark.sql.SparkSession
import scala.collection.Map
import org.apache.spark.sql._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx.lib.ShortestPaths
import scala.reflect.ClassTag
import scala.collection.immutable.ListMap
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.util.control.Breaks._


/**
 * SemanticPartitioner
 *
 */
object BLAP { 
    var subPartitionType: String = ""
    var numOfPartitions: Int = -1
    var subPartitionMode: String = ""
    var dataset: String = ""

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
                                .appName("BLAP")
                                .config("master", "mesos://zk://clusternode1:2181,clusternode2:2181,clusternode3:2181/mesos")
                                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                .config("spark.kryoserializer.buffer.max", "1024m")
                                .config("spark.driver.maxResultSize", "10G")
                                .config("spark.hadoop.dfs.replication", "1") // add it only when saving final partitions. remove it after
                                .getOrCreate()
        import spark.implicits._
        
        val sc = spark.sparkContext
        sc.setCheckpointDir(Constants.HDFS + "checkPoint")
        sc.setLogLevel("ERROR")
        if(args.size < 3){
            println("Missing Arguments")
            println("Arguments: number of partitions, sub partitioning mode, dataset name")
            System.exit(-1)
        }
        
        numOfPartitions = args(0).toInt
        subPartitionMode = args(1)
        dataset = args(2)        
        
        Preprocessor.initializeGraphs(spark)
        val schemaGraph = Loader.loadSchemaGraph(spark).cache

        val instanceGraph = if(dataset == "dbpedia"){
                                Loader.loadInstanceGraph(spark)
                                      .partitionBy(PartitionStrategy.EdgePartition2D, 1600)
                            }
                            else {
                                Loader.loadInstanceGraph(spark)
                                        .partitionBy(PartitionStrategy.EdgePartition2D, 1600)
                                  //    .partitionBy(PartitionStrategy.EdgePartition2D, 400)
                            }


        Preprocessor.computeMeasures(sc, instanceGraph, schemaGraph)
        
        val importanceMap = Loader.loadSchemaImportance() //load betweenness centrality
        val schemaMap = schemaGraph.vertices.collectAsMap
        val centroids = findCentroids(importanceMap, schemaMap, numOfPartitions)
        val instanceCount = sc.textFile(Constants.instancePath).count
        val finalClusters = rdfCluster(spark, instanceGraph, schemaGraph, instanceCount)
        if(!HdfsUtils.hdfsExists(Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal")) {
            partitionClusters(spark, finalClusters)
            if(!HdfsUtils.hdfsExists(Constants.nodeIndexFile  + "_" + numOfPartitions)) {
                generateNodeIndex(spark)
            }
            subPartitionClusters(spark, subPartitionMode)
        }
        spark.stop()
    }    
    def clusterSchema(spark: SparkSession, instanceCount: Long) = {
        val schemaGraph = Loader.loadSchemaGraph(spark).cache
        val schemaVertices = schemaGraph.vertices.collectAsMap

        val importanceMap = Loader.loadSchemaImportance() //load betweenness centrality
        val centroids = findCentroids(importanceMap, schemaVertices, numOfPartitions)
        var weightedSchema: Graph[String, (String, Double)] = null
        
        if(!HdfsUtils.hdfsExists(Constants.weightedVertices + "_" + numOfPartitions) && !HdfsUtils.hdfsExists(Constants.weightedEdges + "_" + numOfPartitions)) {
            weightedSchema = createWeightedGraph(spark, schemaGraph, importanceMap, numOfPartitions)
            val weightedVertices = weightedSchema.vertices
            val weightedEdges = weightedSchema.edges
           // Preprocessor.saveWeightedGraph(weightedVertices, weightedEdges, numOfPartitions)
        }
        else {
            weightedSchema = Loader.loadWeightedGraph(spark.sparkContext, numOfPartitions)
        }        
        
        val nonCentroids = schemaVertices.map(_._1).toSeq

        val result = if(!HdfsUtils.hdfsExists(Constants.shortestPaths + "_" + numOfPartitions)) {
            // weithted shortest paths for every node to every centroid
            val shortestPaths = WeightedShortestPath.run(weightedSchema, nonCentroids)
                                                .vertices
                                                .filter(x => centroids.contains(x._1))

            shortestPaths.map{case(cId, (vMap)) => (vMap.map(x => (x._1, scala.collection.immutable.Map(cId -> x._2))))}
                            .flatMap(m => m)
                            .reduceByKey(_++_)
        }
        else {
            Loader.loadShortestPaths(spark.sparkContext, numOfPartitions)            
        }

       // if(!HdfsUtils.hdfsExists(Constants.shortestPaths + "_" + numOfPartitions))                
         //   Preprocessor.saveShortestPathVertices(result, numOfPartitions)
    
        // val result = Loader.loadShortestPaths(spark.sparkContext, numOfPartitions)         
        
        val diffs = result.filter(node => !node._2.isEmpty)
                            .flatMap(node => dependenceDiff(importanceMap, node._2))

        val maxDiff = diffs.max
        val minDiff = diffs.min

        val schemaCluster = result.filter(node => !node._2.isEmpty)
                                    .map(node =>(node._1, dependence(importanceMap, node._2, maxDiff, minDiff)))
                                    //filter out centroids because give infinity dependence
                                    .filter{case(node, dependenceMap) => !centroids.contains(node)}
                                    .collect

        val clusterTriples = createClusters(schemaCluster, schemaVertices, instanceCount)

        //Array with schema edges
        // val schemaEdges = schemaGraph.triplets
        //                                 .map(t => (t.srcAttr, t.dstAttr, t.attr._1))
        //                                 .distinct
        //                                 .collect

        // val schemaClusters = spark.sparkContext.parallelize(clusterTriples.map(x => x._2.map(y => (x._1, y))).flatten.toSeq)

        // val validSchemaClusters = schemaClusters.map{case(cId, triple) => {
        //                                                     if(schemaEdges.contains(triple))
        //                                                         (cId, replaceVertexIds(triple, schemaVertices))
        //                                                     else
        //                                                         (cId, replaceVertexIds((triple._2, triple._1, triple._3), schemaVertices))
        //                                             }} 
        // val extendedSchemaCluster = validSchemaClusters.map{case(cId, curEdge) => 
        //                                                                 extendCluster(cId, 
        //                                                                                 curEdge, 
        //                                                                                 schemaEdges)
        //                                                             }
        //                                                 .flatMap(list => list)
        //                                                 .distinct

        // val emptyCentroids = centroids.diff(extendedSchemaCluster.map(_._2).distinct.collect)
        // val extendedCentroids = spark.sparkContext.parallelize(emptyCentroids.map(cId => extendCentroid(cId, schemaVertices(cId), schemaEdges)))                                                
        //                             .flatMap(list => list)
        //                             .distinct

        // val finalSchemaCluster = extendedSchemaCluster.union(extendedCentroids).distinct
        // finalSchemaCluster.saveAsTextFile(Constants.HDFS + Constants.schemaClusterFile + "_" + numOfPartitions)
    }

    /**
    * Creates index of class - partition
    */
    def createClassIndex(schemaCluster: Map[Long, Array[String]], centroids: Seq[Long]) = {
        val centroidMap = centroids.zipWithIndex.toMap
        val mapWriter = new PrintWriter(new File(Constants.centroidMapFile + "_" + numOfPartitions))
        
        centroidMap.foreach{case(cId, id) => {
            mapWriter.write(cId + "," + id + "\n")
        }}
        mapWriter.close

        val pw = new PrintWriter(new File(Constants.classIndexFile + "_" + numOfPartitions + "_" + subPartitionMode))
        var classIndex = Map[String, Array[Long]]()

        schemaCluster.foreach{case(cId, nodes) => {
            nodes.foreach(n => {
                if(classIndex.contains(n)){
                    classIndex = classIndex + (n -> (classIndex(n) :+ cId))
                }
                else {
                    classIndex = classIndex + (n -> Array(cId))   
                }
            })
        }}
        classIndex.foreach{case(node, cIds) => {
            pw.write(node + "\t" + cIds.map(centroidMap(_)).distinct.mkString(",") + "\n")
        }}
        pw.close
    }

    def createClusters(dependences: Seq[(Long, Map[Long, (Double, Seq[(Long, Long, String)])])],
                            schemaVertices: Map[Long, String], instanceCount: Long): Map[Long, Array[(Long, Long, String)]] = {
        var clusterCounter = Map[Long, Long]()
        var clusterNodes = Map[Long, Array[Long]]()
        var clusterTriples = Map[Long, Array[(Long, Long, String)]]()

        val centroidMap = aggregateCentroids(dependences)
        val schemaNodeCount = Loader.loadSchemaNodeCount()
        val threshold = instanceCount / numOfPartitions //schemaNodeCount.map(_._2).reduce(_+_)/numOfPartitions
        var thresholdError = threshold / numOfPartitions //computeThresholdError(dependences, schemaVertices, threshold)//threshold / numOfPartitions
        val centroids = centroidMap.keys.toSeq
        var clusteredNodes = Array[Long]()

        println("threshold " + threshold)
        println("threshold error " + thresholdError)

        //initialize maps
        centroids.foreach(c => {
            clusterCounter = clusterCounter + (c -> 0)
            clusterNodes = clusterNodes + (c -> Array[Long]())
            clusterTriples = clusterTriples + (c -> Array[(Long, Long, String)]())
        })

        val array = Array("<http://www.w3.org/2004/02/skos/core#Concept>", "<http://www.w3.org/2002/07/owl#Class>", "<Version 3.8>", "<http://dbpedia.org/ontology/>")
        val intialValidNodes = schemaVertices.keys.toSet.filter(x => !centroids.contains(x) && !array.contains(x)) //all nodes except centroids and invalid
        var leftoutNodes = intialValidNodes.toSet
        var leftoutSize = leftoutNodes.map(schemaVertices(_)).map(schemaNodeCount.getOrElse(_, 0)).reduce(_+_)
        println(" valid nodes ")
        intialValidNodes.foreach(x => println(schemaVertices(x)))

        println("leftout size " + leftoutSize)
        while(leftoutSize != 0) {
            centroidMap.foreach{case(cId, dependences) => {
                dependences.foreach{case(depValue, node, path) => {
                    breakable{                    
                        //if node is clustered continue
                        if(clusteredNodes.contains(node) || array.contains(node))
                            break
                        //current centrois nodes
                        val curCentroidNodes = clusterNodes.getOrElse(cId, Array())
                        //find nodes that current centroid does not contain
                        val newNodes: Seq[Long] = path.map{case(src, dst, pred) => Array(src, dst)}
                                                        .flatten.distinct
                                                        .filter(x => !curCentroidNodes.contains(x))
                        val newNodesize = if(!newNodes.isEmpty) {
                            newNodes.map(n => schemaNodeCount.getOrElse(schemaVertices(n), 0))
                                    .reduce(_+_) 
                        }
                        else { //no nodes to enter 
                            clusteredNodes = clusteredNodes :+ node
                            break //continue
                        }
                        val curClusterSize = clusterCounter(cId)
                        //if incoming nodes fit into cluster OR cluster is empty
                        if(curClusterSize + newNodesize < threshold || curClusterSize == 0) { 
                            clusterNodes = clusterNodes + (cId -> (clusterNodes(cId) ++ newNodes))
                            clusterCounter = clusterCounter + (cId -> (clusterCounter(cId) + newNodesize))
                            clusterTriples = clusterTriples + (cId -> (clusterTriples(cId) ++ path))
                            clusteredNodes = clusteredNodes :+ node
                        }
                        //if cluster is full use thresholdError
                        else if (curClusterSize + newNodesize < threshold + thresholdError) {
                            clusterNodes = clusterNodes + (cId -> (clusterNodes(cId) ++ newNodes))
                            clusterCounter = clusterCounter + (cId -> (clusterCounter(cId) + newNodesize))
                            clusterTriples = clusterTriples + (cId -> (clusterTriples(cId) ++ path))
                            clusteredNodes = clusteredNodes :+ node
                        }
                    }
                }}
            }}

            leftoutNodes = leftoutNodes -- clusteredNodes.toSet
            println("leftout classes " + leftoutNodes.size)
            println(leftoutNodes.map(schemaVertices(_)).mkString)
            leftoutSize = if(!leftoutNodes.isEmpty){
                leftoutNodes.map(schemaVertices(_)).map(schemaNodeCount.getOrElse(_, 0)).reduce(_+_)
            }
            else {
                0
            }
            println("leftout triples size " + leftoutSize)

            println("cluster Distribution")
            clusterCounter.foreach(println)

            thresholdError = thresholdError + thresholdError/numOfPartitions
            println("updated thresholdError " + thresholdError)
        }

        println("clustered classes " + clusteredNodes.size)
        println("total claases " + schemaVertices.size)
        createClassIndex(clusterNodes.map{case(cId, nodes) => (cId, nodes.map(schemaVertices(_)))}, centroids)
        return clusterTriples
    }

    def computeThresholdError(dependences: Seq[(Long, Map[Long, (Double, Seq[(Long, Long, String)])])],
                            schemaVertices: Map[Long, String], threshold: Long): Long = {
        var clusterCounter = Map[Long, Long]()
        var clusterNodes = Map[Long, Array[Long]]()
        var clusterTriples = Map[Long, Array[(Long, Long, String)]]()

        val centroidMap = aggregateCentroids(dependences)
        val schemaNodeCount = Loader.loadSchemaNodeCount()
        val threshold = schemaNodeCount.map(_._2).reduce(_+_)/numOfPartitions
        var thresholdError = threshold / numOfPartitions
        val centroids = centroidMap.keys.toSeq
        var clusteredNodes = Array[Long]()

        println("Compute threshold")
        println("threshold " + threshold)
        println("threshold error " + thresholdError)

        //initialize maps
        centroids.foreach(c => {
            clusterCounter = clusterCounter + (c -> 0)
            clusterNodes = clusterNodes + (c -> Array[Long]())
            clusterTriples = clusterTriples + (c -> Array[(Long, Long, String)]())
        })
        // while(clusterNodes.map(_._2).reduce(_++_).distinct.size < schemaVertices.size){            
            centroidMap.foreach{case(cId, dependences) => {
                dependences.foreach{case(depValue, node, path) => {
                    breakable{                    
                        //if node is clustered continue
                        if(clusteredNodes.contains(node)){
                            println("contains node")
                            break
                        }
                        //current centrois nodes
                        val curCentroidNodes = clusterNodes.getOrElse(cId, Array())
                        //find nodes that current centroid does not contain
                        val newNodes: Seq[Long] = path.map{case(src, dst, pred) => Array(src, dst)}
                                                        .flatten.distinct
                                                        .filter(x => !curCentroidNodes.contains(x))
                        val newNodesize = if(!newNodes.isEmpty) {
                            newNodes.map(n => schemaNodeCount.getOrElse(schemaVertices(n), 0))
                                    .reduce(_+_) 
                        }
                        else { // no nodes to enter 
                            println("no nodes to enter")
                            break //continue
                        }
                        val curClusterSize = clusterCounter(cId)
                        //if incoming nodes fit into cluster OR cluster is empty
                        if(curClusterSize + newNodesize < threshold || curClusterSize == 0) { 
                            clusterNodes = clusterNodes + (cId -> (clusterNodes(cId) ++ newNodes))
                            clusterCounter = clusterCounter + (cId -> (clusterCounter(cId) + newNodesize))
                            clusterTriples = clusterTriples + (cId -> (clusterTriples(cId) ++ path))
                            clusteredNodes = clusteredNodes :+ node
                        }
                        //if cluster is full use thresholdError
                        else if (curClusterSize + newNodesize < threshold + thresholdError) {
                            clusterNodes = clusterNodes + (cId -> (clusterNodes(cId) ++ newNodes))
                            clusterCounter = clusterCounter + (cId -> (clusterCounter(cId) + newNodesize))
                            clusterTriples = clusterTriples + (cId -> (clusterTriples(cId) ++ path))
                            clusteredNodes = clusteredNodes :+ node
                        }
                    }
                }}
            }}
            thresholdError = thresholdError + (thresholdError / numOfPartitions)
            println("clustered nodes " + clusteredNodes.distinct.size)
            println("total nodes " + schemaVertices.size)
            println((clusteredNodes.toSet -- schemaVertices.keys.toSet).mkString)
            println(thresholdError)
        // }
        println("clusteres nodes " + clusteredNodes.distinct.size)
        return thresholdError
    }

    def aggregateCentroids(dependences: Seq[(Long, Map[Long, (Double, Seq[(Long, Long, String)])])]): 
                                        Map[Long, Array[(Double, Long, Seq[(Long, Long, String)])]] = {
        var centroidMap = Map[Long, Array[(Double, Long, Seq[(Long, Long, String)])]]()
        dependences.foreach{case(node, depMap) => {
            depMap.foreach{case(cId, (dep, path)) => {
                val depArray: Array[(Double, Long, Seq[(Long, Long, String)])] = Array((dep, node, path))
                if(centroidMap.contains(cId)){
                    centroidMap = centroidMap + (cId -> (centroidMap(cId) ++ depArray))
                }
                else {
                    centroidMap = centroidMap + (cId -> depArray)   
                }
            }}
        }}
        centroidMap.map{case(cId, dependences) => (cId, dependences.sortBy(_._1)(Ordering[Double].reverse))}
    }

    /**
    * Choose centroid with max dependence that fit incoming nodes
    */
    def chooseMaxDependence(dependenceMap: Map[Long, (Double, Seq[(Long, Long, String)])],
                            clusterCounter: Map[Long, Long],
                            clusterNodes: Map[Long, Array[Long]],
                            threshold: Long,
                            schemaVertices: Map[Long, String]): Long = {

        val schemaNodeFreq = Loader.loadSchemaNodeCount()
        //sort map descending
        val sortedDependenceMap = ListMap(dependenceMap.toSeq.sortWith(_._2._1 > _._2._1):_*)
        sortedDependenceMap.foreach{case(cId, (dep, path)) => {
            val curCentroidNodes = clusterNodes.getOrElse(cId, Array())
            val incommingNodes: Seq[Long] = path.map{case(src, dst, pred) => Array(src, dst)}
                                                .flatten.distinct //.map(schemaVertices(_))
                                                .filter(x => !curCentroidNodes.contains(x))

            val incommingNodeSize = if(!incommingNodes.isEmpty) {
                incommingNodes.map(n => schemaNodeFreq.getOrElse(schemaVertices(n), 0))
                                .reduce(_+_) 
            }
            else { //no nodes to enter 
                return -1L
            }
            val clusterSize = clusterCounter(cId)
            // if nodes fit into cluster
            if(incommingNodeSize + clusterSize < threshold || clusterSize == 0) { 
                return cId
            }
        }}
        //if incoming nodes does not fit anywhere, choose smallest cluster
        println("smallest")
        val smallestCluster = clusterCounter.toArray.sortWith(_._2 < _._2)(0)._1
        if(dependenceMap.contains(smallestCluster))
            return smallestCluster
        else 
            return -1L
    }
    
    def sparqlgxStatistics(spark: SparkSession) = {
        val sc = spark.sparkContext
        val a = Array("p1f_w3_org_1999_02_22_rdf_syntax_ns_type", "p3_xmlns_com_foaf_0_1_name")
        a.foreach(p => {
            val path = "sparqlgx/dbpedia/" + p
            println(path)
            println(sc.textFile(Constants.HDFS + path).count) 
        })
    }
    def rePartitionClusters(spark: SparkSession) = {
        import spark.implicits._
        val basePath = "lubm-1000_data/cluster/clusters_vp_4/"
        val partitions = HdfsUtils.getDirFilesNames(basePath)

        partitions.foreach(p => {
            if(p.length == 5){
                val path = basePath + p
                val subPartitions = HdfsUtils.getDirFilesNames(path)
                subPartitions.foreach(subPartition => {
                    val subPath = path + "/" + subPartition
                    val rPath = basePath + "r/" + p + "/" + subPartition
                    println(subPath)
                    val fileSize = HdfsUtils.getSize(subPath)
                    if(fileSize > 64.0)
                        println(fileSize)
                    if(fileSize > 64.0) {
                        val data = spark.read.load(Constants.HDFS + subPath).as[(String, String)].rdd
                        val partitions = roundUp(data.partitions.size/3)
                        if(partitions == 0){//10
                            data.repartition(10).toDF().write.format("parquet").save(Constants.HDFS + rPath)    
                        }
                        else {
                            data.repartition(10).toDF().write.format("parquet").save(Constants.HDFS + rPath)
                        }
                    }
                    else if(fileSize > 10.0) {
                        val data = spark.read.load(Constants.HDFS + subPath).as[(String, String)].rdd
                        val partSize = 1
                        val partitions = roundUp(data.partitions.size/5)
                        if(partitions == 0){//5
                            data.repartition(5).toDF().write.format("parquet").save(Constants.HDFS + rPath)    
                        }
                        else {
                            data.repartition(5).toDF().write.format("parquet").save(Constants.HDFS + rPath)
                        }
                        
                    }
                    else if(fileSize > 5.0) {
                        val data = spark.read.load(Constants.HDFS + subPath).as[(String, String)].rdd
                        val partSize = 1
                        val partitions = roundUp(data.partitions.size/5)
                        if(partitions == 0){//5
                            data.repartition(3).toDF().write.format("parquet").save(Constants.HDFS + rPath)    
                        }
                        else {
                            data.repartition(3).toDF().write.format("parquet").save(Constants.HDFS + rPath)
                        }
                    }
                    else {
                        val data = spark.read.load(Constants.HDFS + subPath).as[(String, String)].rdd
                        val partSize = 1
                        val partitions = data.partitions.size //3
                        data.repartition(1).toDF().write.format("parquet").save(Constants.HDFS + rPath)   
                    }    
                })
            }
            // HdfsUtils.removeDirInHDFS(path)
        })

        def roundUp(d: Double) = math.ceil(d).toInt      
    }

    def createSparqlgxData(spark: SparkSession, dataset: String) = {
        Loader.loadBaseLine(spark).rdd.map(t => t._1 + "\t" + t._3 + "\t" + t._2 +" .")
                /*.repartition(1)*/.saveAsTextFile(Constants.HDFS + "sparqlgx_" + dataset)
    }

    def validUri(str: String): Boolean = {str.startsWith("<") && str.endsWith(">")}

    class TestPartitioner(override val numPartitions: Int) extends Partitioner {
        override def getPartition(key: Any): Int = {
            val src = key.toString
            return math.abs(src.hashCode) % numPartitions
        }

        override def equals(other: scala.Any): Boolean = {
            other match {
                case obj : TestPartitioner => obj.numPartitions == numPartitions
                case _ => false
            }
        }
    }
    

    def createSubPartitionMap(): Map[String, Int] = {
        var subPartitionMap = Map[String, Int]()
        val fileSuffix = generateFileSuffix(numOfPartitions)
        fileSuffix.foreach(suffix => {
            val exists = HdfsUtils.hdfsExists(Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal" + "/" + suffix +"/01")
            if(exists) {
                val subPartitions = HdfsUtils.countSubFolders(Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal" + "/" + suffix)
                subPartitionMap = subPartitionMap + (suffix -> subPartitions)
            }
        })
        return subPartitionMap
    }

    /**
    * Reads multiple input files and save them in a single one.
    * Splits uris by tab
    */
    def transformInstanceInputfiles(sc: SparkContext) = {
        val rdd = sc.textFile(Constants.HDFS + "swdf/instances")
                    .filter(!_.startsWith("#"))
                    .map(t => t.split(" "))
                    .map(t => {
                        t(0) + "\t" + t(1) + "\t" + t.drop(2).mkString(" ")//.dropRight(2)
                    })
                    .repartition(1)
        rdd.saveAsTextFile(Constants.HDFS + "tmp_swdf")
    }

    /**
    * Saves all partitioned data in a single parquet file 
    * Note: some data may not be included in baseline because 
    * they do not belong to a class and so they are not partitioned.
    */
    def createBaseline(spark: SparkSession, k: Int) = {
        import spark.implicits._
        val path = Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + k +"/*.parquet"
        val baseline = spark.read.load(path).as[(String, String, String)]
        baseline.distinct.write.format("parquet").save(Constants.HDFS + Constants.baseLine)
        println(baseline.distinct.count)
    }               

    /**
    * reproduce triples with rdf:type for each instance
    */
    def generateTypeTriples(triple: EdgeTriplet[(String, Seq[String]),String]) = {
        val triples = triple.srcAttr._2.map(t => (triple.srcAttr._1, t, Constants.RDF_TYPE)) ++ 
                        triple.dstAttr._2.map(t => (triple.dstAttr._1, t, Constants.RDF_TYPE)) ++ 
                        Seq((triple.srcAttr._1, triple.dstAttr._1, triple.attr))
        triples
    }

    /**
    * Partitions data based on partition label by using a CustomPartitioner
    */
    def partitionClusters(spark: SparkSession, finalClusters: RDD[((String, String, String), String)]) = {
        import spark.implicits._
        //val centroidMap = Loader.loadCentroiMap(numOfPartitions)
        finalClusters.map(x => (x._2, x._1))
                        .partitionBy(new CustomPartitioner(numOfPartitions))
                        .map(x => x._2)
                        .toDF().write.format("parquet").save(Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal")    

        val stats = finalClusters.map(x => (x._2, 1))
                                    .reduceByKey(_+_)
                                    .collect
        
        val sw = new PrintWriter(new File(Constants.statisticsFile + "_" + numOfPartitions))
        stats.foreach{case(partition, size) => {
            if(partition.toString.size == 1)
                sw.write("0"+partition.toString + " " + size + "\n")
            else
                sw.write(partition.toString + " " + size + "\n")
        }}
        sw.close
    }
    
   
    /**
    * Computes which partitions need to split.
    * Returns a map of partitionName, subPartitionSize
    */
    def computeSubPartitioning(spark: SparkSession, partitionNum: Int): Map[String, Int] = {
        val statistics = Loader.loadStatistics(numOfPartitions)
        val sum = statistics.map(_._2).reduce((a, b) => a + b)
        val threshold = sum / partitionNum.toDouble
        val partitions = generateFileSuffix(partitionNum)
               
        //initialize all partitions greater than threshold to split in 2
        var partitionsToSplit = statistics.filter{case (partition, size) => (size >= threshold)}.map{case (partition, size) => (partition, 2)}
        var flag = 1
        while(flag == 1) {
            flag = 0
            partitionsToSplit.foreach{case(partition, subPartitions) => {
                if(statistics(partition) / subPartitions > threshold) {
                    partitionsToSplit = partitionsToSplit + (partition -> (subPartitions + 2))
                    flag = 1
                }
            }}
        }
        statistics.filter{case (partition, size) => (size < threshold)}.foreach{case (partition, size) => {
            partitionsToSplit = partitionsToSplit + (partition -> 0)
        }}

        partitionsToSplit.map{case(partition, size) => (fillStr(partition), size)}.toMap        
    }

    def subPartitionClusters(spark: SparkSession, mode: String) = {
        import spark.implicits._
        val sc = spark.sparkContext
    
        val basePath = Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal"
        val utilPath = Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal"
        val partitions = HdfsUtils.getDirFilesNames(utilPath)
        partitions.foreach(p => {
            val fullPath = basePath + "/" + p
            val partition = Loader.loadClustersWithId(spark, fullPath)
                                    .withColumnRenamed("_1", "s")
                                    .withColumnRenamed("_2", "o")
                                    .withColumnRenamed("_3", "p")
                                    .as[(String, String, String)]
            val start = p.indexOf("part-") + 5
            val partitionName = p.substring(start, start+5)
            println(partitionName)
            val partSize = partition.rdd.partitions.size
            println(partSize)
            partition.rdd.repartition(roundUp(partSize/7.0))
                        .filter{case(s, o, p) => p.size < 150}
                        .toDF.as[(String, String, String)]
                        .map{case(s, o, p) => (s, o, uriToStorage(p))}
                        .write.partitionBy("_3").parquet(basePath + "/" + partitionName)
            //remove
          //  HdfsUtils.removeFilesInHDFS(utilPath + "/" + p)
        })
    }
    
    def uriToStorage(uri: String) = {
        uri.replaceAll("[^A-Za-z0-9]", "_").toLowerCase
    }

    /**
    * Repartition clusters by using the default spark partitioner, 
    * such as semantic partitions will be splitted into more files.
    */
    def rePartitionClusters(spark: SparkSession, partitionsToSplit: Map[String, Int]) = {
        import spark.implicits._
        partitionsToSplit.foreach{case (partition, size) => {
            val basePath = Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal" + "/"
            val partitionPath = basePath + partition
            val files = HdfsUtils.getDirFilesNames(partitionPath)
            files.foreach(file => {
                val fileSize = HdfsUtils.getSize(partitionPath + "/" + file)
                println(fileSize)
                if(fileSize > 128.0){
                    val ds = Loader.loadClustersWithId(spark, Constants.HDFS + partitionPath + "/" + file)
                    val startIndex = file.indexOf("-")
                    var cleanFileName = ""
                    if(size == 0)
                        cleanFileName = "00000"
                    else   
                        cleanFileName = file.substring(startIndex + 1, startIndex + 6)
                    ds.write.format("parquet")
                            .save(Constants.HDFS + partitionPath + "/" + cleanFileName)
                    HdfsUtils.removeFilesInHDFS(partitionPath + "/" + file)
                }
                else {
                    val startIndex = file.indexOf("-")
                    var cleanFileName = ""
                    if(size == 0)
                        cleanFileName = "00000"
                    else
                        cleanFileName = file.substring(startIndex + 1, startIndex + 6)
                    HdfsUtils.moveFile(partitionPath + "/" + file, partitionPath + "/" + cleanFileName)
                }
            })
        }}
       
    }

    /**
    * Helper function that search for dependence around a node.
    * **Debugging**
    */
    def nodeDependence(spark: SparkSession,  schemaGraph: Graph[String, (String, Double)], result: RDD[(VertexId, scala.collection.immutable.Map[VertexId,(Double, Seq[(VertexId, VertexId, String)])])]) = {
        val sc = spark.sparkContext
        val schemaVertices = schemaGraph.vertices.collectAsMap
        val id = schemaVertices.filter(_._2 =="http://dbpedia.org/ontology/Settlement").keys.toArray
        println("id " + id(0))
        val ids = schemaGraph.triplets
                                .filter(t => t.srcAttr == "http://dbpedia.org/ontology/Settlement")//&& clusterIds.contains(t.dstAttr))
                                .distinct
                                .map(t => t.dstId)
                                .take(10)
        
        val importanceMap = Loader.loadSchemaImportance() //load betweenness centrality

        val diffs = result.filter(node => !node._2.isEmpty)
                            .flatMap(node => dependenceDiff(importanceMap, node._2))

        val maxDiff = diffs.max
        val minDiff = diffs.min

        val dependenceVertices = result.filter(node => !node._2.isEmpty)
                                        .map(node => (node._1, dependence(importanceMap, node._2, maxDiff, minDiff)))

        ids.map(schemaVertices(_)).foreach(println)

        println("Person neighboors")
        dependenceVertices.filter(v => ids.contains(v._1))
                            .map{case(cId, m) => 
                                    (schemaVertices.get(cId).get, 
                                        m.map{case(k, v) => 
                                            (schemaVertices.get(k).get, 
                                                (v._1, v._2.map(triple => (schemaVertices.get(triple._1).get, schemaVertices.get(triple._2).get, triple._3)))
                                            )
                                        }
                                    )
                                }
                            .collect
                            .foreach(println)
    }

    def computePartitionStatistics(spark: SparkSession) = {
        val basePath = Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal" + "/"
        val baseUtilsPath = Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal" + "/"
        val sw = new PrintWriter(new File(Constants.partitionStatFile + "_" + numOfPartitions + "_" + subPartitionMode + "_bal"))
        val partitions = HdfsUtils.getDirFilesNames(baseUtilsPath)

        partitions.foreach(p => {
            val fullPath = basePath + p //+"/*"
            val cluster = spark.read.load(fullPath)
            val c = cluster.count
            sw.write(p + " " + c + "\n")              
        })
        sw.close
    }

    /**
    *  Writes in a file statistics about partitioning.
    */
    def computeDataStatistics(spark: SparkSession, k: Int) = {
        val basePath = Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal" + "/"
        val baseUtilsPath = Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal" + "/"
        val sw = new PrintWriter(new File(Constants.dataStatisticsFile  + "_" + numOfPartitions + "_" + subPartitionMode + "_bal" ))
        val partitions = HdfsUtils.getDirFilesNames(baseUtilsPath)

        partitions.foreach(p => {
            val fullPath = basePath + p //+"/*"
            val cluster = spark.read.load(fullPath)
            val c = cluster.count
            sw.write(p + " " + c + "\n")              
        })

        val clusteredData = spark.read.load(basePath + "/*.parquet").cache
        val dCount = clusteredData.distinct.count
        val count = clusteredData.count
        
        sw.write("Partitioned distinct triples: " + dCount + "\n")
        sw.write("Partitioned total triples: " + count + "\n")
        sw.write("Replication: " + count/dCount.toFloat + "\n")
        sw.close
        clusteredData.unpersist()
    }

    // /**
    // * Creates index of class - partition
    // */
    // def createClassIndex(schemaClusters: RDD[(Long, (String, String, String))], centroids: Seq[Long], emptyCentroids: Array[(Long, String)]) = {
    //     val centroidMap = centroids.zipWithIndex.toMap
    //     val mapWriter = new PrintWriter(new File(Constants.centroidMapFile + "_" + numOfPartitions))
        
    //     centroidMap.foreach{case(cId, id) => {
    //         mapWriter.write(cId + "," + id + "\n")
    //     }}
    //     mapWriter.close

    //     val classIndex = schemaClusters.flatMap{case(cId, (s, o, p)) => 
    //                                                 Array((s, Array(centroidMap(cId))), (o, Array(centroidMap(cId))))
    //                                             }
    //                                     .reduceByKey(_++_)  //index empty centroids
    //                                     .collect.toArray ++ emptyCentroids.map{case(id, uri) => (uri, Array(centroidMap(id)))}

    //     val pw = new PrintWriter(new File(Constants.classIndexFile + "_" + numOfPartitions))
    //     var i = 0
    //     while(i < classIndex.length) {
    //         pw.write(classIndex(i)._1 + "\t" + classIndex(i)._2.distinct.mkString(",") + "\n")
    //         i+=1
    //     }
    //     pw.close
    // }



    def fillStr(str: String) = {List.fill(5-str.length)("0").mkString + str}
    
    /**
    * Builds an index of node to class
    */
    def generateNodeIndex(spark: SparkSession) = {
        import spark.implicits._
        // val fullPath = Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "_bal" + "/*/*"
        val cluster = Loader.loadBaseLine(spark)// loadClusters(spark, fullPath).distinct
        val nodeIndex = cluster.rdd.filter{case(s, o, p) => p == Constants.RDF_TYPE}
                                    .map{case(s, o, p) => {
                                        (s, o)
                                    }}
        
        val nodeIndexDf = nodeIndex.map(node => Types.NodeIndex(node._1, node._2)).toDF()
        nodeIndexDf.write.format("parquet").save(Constants.HDFS + Constants.nodeIndexFile + "_" + numOfPartitions)
        
        // val pw = new PrintWriter(new File(Constants.nodeIndexFileLocal + "_" + numOfPartitions))
        
        // nodeIndex.collect.foreach{case(node, t) => {
        //     pw.write(node + "\t" + t + "\n")
        // }}
        // pw.close
    }

    
    /**
    * given a list of partitions, frequency_of_node and statistics on partitions
    * finds the smallest partition
    * **Probably not in use**
    */
    def findSmallestPartition(partitions: Seq[(String, Int)], statistics: Seq[(String, Int)]): Seq[String] = {
        val maxOccPartition = partitions.maxBy(_._2)
        val statisticMap = statistics.toMap
        var maxOccPartitionStat = statisticMap(maxOccPartition._1)
        var returnValue = Seq(maxOccPartition._1)
        partitions.filter{case(partition, size) => size == maxOccPartition._2}
                    .foreach{case(partition, size) => {
                        val curPartitionStat = statisticMap(partition)
                        if(curPartitionStat < maxOccPartitionStat){
                            maxOccPartitionStat = curPartitionStat
                            returnValue = Seq(partition)
                        }

                    }}
        returnValue
    }

    /**
    * generates file sufixes for the given partition number.
    * used for parsing files in HDFS
    */    
    def generateFileSuffix(k: Int): Seq[String] = {
        var fileSuffix = Seq[String]()
        var i = k
        for(i <- k - 1 to 0 by -1){
            val kStr = i.toString
            if(kStr.size < 2)
                fileSuffix = fileSuffix :+ ("0" + kStr)
            else
                fileSuffix =  fileSuffix :+ kStr
        }
        fileSuffix
    }

    class ClassPartitioner(override val numPartitions: Int) extends Partitioner {
        override def getPartition(key: Any): Int = {
            return math.abs(key.toString.hashCode) % numPartitions
        }
        override def equals(other: scala.Any): Boolean = {
            other match {
                case obj : ClassPartitioner => obj.numPartitions == numPartitions
                case _ => false
            }
        }
    }

    /**
    * Custom partitioner by using partitioning label attached to triples.
    */
    class CustomPartitioner(override val numPartitions: Int) extends Partitioner {
        override def getPartition(key: Any): Int = {
            return key.toString.toInt
        }
        override def equals(other: scala.Any): Boolean = {
            other match {
                case obj : CustomPartitioner => obj.numPartitions == numPartitions
                case _ => false
            }
        }
    }

    /**
    * Horizontal partitioner based on hash of the given key. (subj OR obj)
    */
    class HorizontalPartitioner(override val numPartitions: Int) extends Partitioner {
        override def getPartition(key: Any): Int = {
            //modification of hash result to avoid negatives
            return math.abs(key.toString.hashCode) % numPartitions
        }

        override def equals(other: scala.Any): Boolean = {
            other match {
                case obj : HorizontalPartitioner => obj.numPartitions == numPartitions
                case _ => false
            }
        }
    }

    //compute hash of String
    def hashValue(str: String): Int = {
        var hash = 7
        for( i <- 0 to str.length()-1){
            hash = hash*31 + str.charAt(i)
        }
        hash
    }

    def roundUp(d: Double) = math.ceil(d).toInt

    
    
    def partitionInstances(sc: SparkContext, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String]): RDD[((String, String, String), String)] = {
        val classIndex = Loader.loadClassIndexBal(numOfPartitions, subPartitionMode)
        val centroidMap = Loader.loadCentroiMap(numOfPartitions)//.map(x => (x._2, x._1))
        //val schemaCluster = Loader.loadSchemaCluster(sc, numOfPartitions).map{case(t, cId) => (t, centroidMap(cId).toString)}
        
        val brIndex = sc.broadcast(classIndex)
        val brMap = sc.broadcast(centroidMap)

        val partitionedInstances = instanceGraph.triplets.filter(t => !t.srcAttr._2.isEmpty || !t.dstAttr._2.isEmpty)
                                                .map(t => ((t.srcAttr._1, t.dstAttr._1, t.attr), (t.srcAttr._2 ++ t.dstAttr._2).toArray.distinct))
                                                .map{case(t, types) => (t, types.map(brIndex.value.getOrElse(_, Array())))}
                                                .map{case(t, partitions) => (partitions.reduce((a, b) => (a ++ b)).distinct.map(p => (t, p)))}
                                                .flatMap(t => t)
                                                .distinct

        //Create and partition RDF_TYPE triples
        val typeTriples = instanceGraph.triplets.filter(t => !t.srcAttr._2.isEmpty || !t.dstAttr._2.isEmpty)
                                        .map(t => t.srcAttr._2.map(typeUri => (t.srcAttr._1, Constants.RDF_TYPE, typeUri))
                                                ++ t.dstAttr._2.map(typeUri => (t.dstAttr._1, Constants.RDF_TYPE, typeUri)))
                                        .flatMap(t => t).distinct

        val partitionTypeTriples = typeTriples.map{case(t) => (t, brIndex.value.getOrElse(t._3, Array()))}
                                                .filter(!_._2.isEmpty)
                                                .map{case(t, partitions) => partitions.map(p => (t, p))}
                                                .flatMap(t => t)
                                                .map{case(t, p) => ((t._1, t._3, t._2), p)}
                                                .distinct
    
        val labelTriples = instanceGraph.triplets.filter(t => (!t.srcAttr._3.isEmpty && !t.srcAttr._2.isEmpty) || (!t.dstAttr._3.isEmpty && !t.dstAttr._2.isEmpty))
                                        .map(t => populateLabels(t.srcAttr) ++ populateLabels(t.dstAttr))
                                        .flatMap(t => t)
                                        .map{case(t, types) => (t, types.map(brIndex.value.getOrElse(_, Array())))}
                                        .filter(!_._2.isEmpty)
                                        .map{case(t, partitions) => (partitions.reduce((a, b) => (a ++ b)).distinct.map(p => (t, p)))}
                                        .flatMap(t => t)
                                        .distinct

        val finalClusters = /*schemaCluster.union(*/partitionTypeTriples.union(partitionedInstances).union(labelTriples).distinct
        return finalClusters
    }

    def populateLabels(srcAttr: (String, Seq[String], Seq[(String, String)])): Seq[((String, String, String), Seq[String])] = {
        val uri = srcAttr._1
        val types = srcAttr._2
        val labels = srcAttr._3
        labels.map{case(p, o) => (uri, o, p)}.map(triples => (triples, types))
    }

    /**
    * Main function of partitioning procedure.
    */
    def rdfCluster(spark: SparkSession, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String], schemaGraph: Graph[String, (String, Double)], instanceCount: Long): RDD[((String, String, String), String)] = {
        val sc = spark.sparkContext
      //  println("instance count " + instanceCount)
        //Partitioning of schema graph
        if(!new File(Constants.classIndexFile + "_" + numOfPartitions + "_" + subPartitionMode).exists)
            clusterSchema(spark, instanceCount)
        partitionInstances(sc, instanceGraph)
    } 

    def flattenTriple(triple: Tuple3[String, String, String]): Seq[(String, (Tuple3[String, String, String], Int))] = {
        if(triple._2.contains("\""))
            Seq((triple._1, (triple, 1)))
        else
            Seq((triple._1, (triple, 1)), (triple._2, (triple, 2)))
    }

    /**
    * Helper function
    * Replace vertex ids of a triple with uris
    */
    def replaceVertexIds(triple: Tuple3[Long, Long, String], map: Map[Long, String]): (String, String, String) = {
        (map(triple._1), map(triple._2), triple._3)
    }

    /**
    * Used after initial schema partitiong.
    * Replicates each node with all its incoming/outgoing edges
    */
    def extendCluster(cId: Long, curEdge: Tuple3[String, String, String], schemaEdges: Array[(String, String, String)]) = {
        schemaEdges.filter(edge => Seq(curEdge._1, curEdge._2).contains(edge._1)
                                    || Seq(curEdge._1, curEdge._2).contains(edge._2))
                    .map(triple => (triple, cId))      
    }

    /**
    * Helper function
    * Extends an empty partition by its nodes incoming/outgoing triples
    */
    def extendCentroid(cId: Long, centroidUri: String, schemaEdges: Array[(String, String, String)]) = {
        schemaEdges.filter(edge => centroidUri == edge._1
                                    || centroidUri == edge._2)
                    .map(triple => (triple, cId))      
    }

    /**
    * add weights to the schema graph
    */
    def createWeightedGraph(spark: SparkSession, schemaGraph: Graph[String, (String, Double)], importanceMap: Map[Long, Double], k: Int) = {
        val sc = spark.sparkContext
        val cc = Loader.loadCC(sc).map(x => ((x._1, x._2, x._3),(x._4, x._5))).collectAsMap //load cardinalities CONVERT TO MAP
        val c = schemaGraph.numVertices //number of schema vertices
        val bothDirectionalSchema = Graph(schemaGraph.vertices, schemaGraph.edges.union(schemaGraph.edges.reverse))
        val brCC = sc.broadcast(cc)
        val weightedEdges= bothDirectionalSchema.triplets
                                         .map(triplet => addWeight(c, triplet, importanceMap, cc)) //create weighted graph edges 

        Graph(bothDirectionalSchema.vertices, weightedEdges) //create weighted schema graph
    }

    /**
    * Checks if path between 2 nodes exists
    */
    def pathExists(graph: Graph[String, (String, Double)], srcId: VertexId, centroids: Seq[VertexId]) = {
        val result = ShortestPaths.run(graph, centroids)
        val shortestPaths = result.vertices
                                    .filter({case(vId, _) => vId == srcId})
                                    .count
        if(shortestPaths > 0)
            true
        false
    }

    /**
    * Helper
    * used of calculation of max and min dependence. (Normalization)
    */
    def dependenceDiff(importanceMap: Map[Long, Double], centroidMap: Map[Long, Tuple2[Double, Seq[Tuple3[Long, Long, String]]]]) = {
        centroidMap.map(x => calculateDependenceDiff(importanceMap(x._1), x._2))
    }

    /**
    * Helper
    * used of calculation of max and min dependence. (Normalization)
    */
    private def calculateDependenceDiff(srcBC: Double, centroidTuple: Tuple2[Double, Seq[Tuple3[Long, Long, String]]]) = {
        (srcBC - centroidTuple._1)
    }
    
    /**
    * calculates dependence for each centroid 
    * srcBC: Source vertex BC
    * centroidMap: [ cetroidID, ( Sum(BC,CC) ,path to each centroid) ]
    * returns: Map[CentroidId, Dependence]
    */
    def dependence(importanceMap: Map[Long, Double], 
                    centroidMap: Map[Long, Tuple2[Double, Seq[Tuple3[Long, Long, String]]]], 
                    max: Double, min: Double) = {
        centroidMap.map(x => (x._1, (calculateDependence(importanceMap(x._1), x._2, max, min), x._2._2)))
    }
    
    /**
    * calculates dependence of 2 schema nodes 
    * srcBC: Source vertex BC
    * centroidTuple: [( Sum(BC,CC) ,path to centroid) ]
    * returns: Dependence
    */
    private def calculateDependence(srcBC: Double, centroidTuple: Tuple2[Double, Seq[Tuple3[Long, Long, String]]], max: Double, min: Double) = {
        val pathSizeSquared = scala.math.pow(centroidTuple._2.size, 2) //path size ^ 2
        val diff = (srcBC - centroidTuple._1)
        val normDiff = (diff - min) / (max - min)
        ((1 / pathSizeSquared).toDouble * normDiff)
    }

    /**
    * Helper function to map triple to centroid Id (used for flattening)
    */
    def mapIdToTriples(centroidTuple: Tuple2[Long, Tuple2[Double, Seq[Tuple3[Long, Long, String]]]]) = {
        centroidTuple._2._2.map(triple => (centroidTuple._1, triple))
    }

    /**
    * computes cardinality closeness of an edge
    */
    def cardinalityCloseness(triplet: EdgeTriplet[String, (String, Double)], c: Long, cc: Map[(String, String, String), (Int, Int)]): Double = {
        val t1 = (triplet.srcAttr, triplet.attr._1, triplet.dstAttr)
        val t = if (cc.keySet.contains(t1)) t1 else (t1._3, t1._2, t1._1)
        val cardinalities = cc.getOrElse(t, (1, 0))
        val constant = (1 + c.toDouble) / c.toDouble
        val distinctFreq = cardinalities._2.toDouble
        val freq = cardinalities._1.toDouble
        val ccValue = constant + (distinctFreq / freq)
        if(distinctFreq == 0)
            constant
        else
            ccValue
    }

    /**
    * adds weight to a triple
    * weight: (destBC/CC)
    */
    def addWeight(c: Long ,triplet: EdgeTriplet[String, (String, Double)], importance: Map[Long, Double], cc: Map[(String, String, String), (Int, Int)]): Edge[(String, Double)] = {
        val destImportance = importance(triplet.dstId).toDouble
        val curCC = cardinalityCloseness(triplet, c, cc)
        val newEdge = new Edge[(String, Double)]
        newEdge.attr = (triplet.attr._1, destImportance / curCC)
        newEdge.srcId = triplet.srcId
        newEdge.dstId = triplet.dstId
        newEdge
    }
    
    /**
    * Min max normalization
    */
    def normalizeValue(value: Double, min: Double, max: Double) = (value - min) / (max - min)
    
    /**
    * finds top k centroids based on BC
    */
    def findCentroids(importance: Map[Long,Double], schemaMap : Map[Long, String],k: Int) = {
        importance.filter(x => schemaMap(x._1).contains("http")).toSeq.sortWith(_._2 > _._2).map{case(x,y) => x}.take(k)
    }
}