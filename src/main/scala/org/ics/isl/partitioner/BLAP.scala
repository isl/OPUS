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
    var dataset: String = ""
    var hdfs = ""
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
                                .appName("BLAP")
                                .config("master", "mesos://zk://clusternode1:2181,clusternode2:2181,clusternode3:2181/mesos")
                                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                .config("spark.kryoserializer.buffer.max", "1024m")
                                .config("spark.driver.maxResultSize", "10G")
                                .config("spark.hadoop.dfs.replication", "1")
                                .getOrCreate()
        import spark.implicits._
        
        val sc = spark.sparkContext
        sc.setCheckpointDir(hdfs + "checkPoint")
        sc.setLogLevel("ERROR")
        if(args.size < 3){
            println("Missing Arguments")
            println("Arguments: number of partitions, dataset name, hdfs base path, insances path, schema path")
            System.exit(-1)
        }
        
        numOfPartitions = args(0).toInt
        dataset = args(1)        
        hdfs = args(2)

        val instancePath = args(3)
        val schemaPath = args(4)
        val mkdir = "mkdir " + dataset + "_local"

        if(!hdfs.endsWith("/"))
            hdfs = hdfs + "/"

        Preprocessor.initializeGraphs(spark, dataset, hdfs, schemaPath, instancePath)
        val schemaGraph = Loader.loadSchemaGraph(spark, dataset, hdfs).cache

        val instanceGraph = Loader.loadInstanceGraph(spark, dataset, hdfs)
                                    .partitionBy(PartitionStrategy.EdgePartition2D, 1600)
                        


        Preprocessor.computeMeasures(sc, instanceGraph, schemaGraph, dataset, hdfs)
        val instanceCount = sc.textFile(instancePath).count

        val finalClusters = rdfCluster(spark, instanceGraph, schemaGraph, instanceCount, dataset, hdfs)
        if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.clusters + "_" + numOfPartitions + "_bal")) {
            partitionClusters(spark, finalClusters, dataset, hdfs)
            if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.nodeIndexFile  + "_" + numOfPartitions)) {
                generateNodeIndex(spark, dataset, hdfs)
            }
            subPartitionClusters(spark, dataset, hdfs)
        }
        if(!new File(dataset + Constants.partitionStatFile + "_" + numOfPartitions + "_bal").exists)
            computePartitionStatistics(spark)
        spark.stop()
    }    

    def computePartitionStatistics(spark: SparkSession) = {
        val basePath = hdfs + dataset + Constants.clusters + "_" + numOfPartitions + "/"
        val baseUtilsPath = dataset + Constants.clusters + "_" + numOfPartitions + "/"
        val sw = new PrintWriter(new File(dataset + Constants.partitionStatFile + "_" + numOfPartitions + "_bal"))
        val partitions = HdfsUtils.getDirFilesNames(hdfs + baseUtilsPath)

        partitions.foreach(p => {
            val fullPath = basePath + p //+"/*"
            val cluster = spark.read.load(fullPath)
            val c = cluster.count
            sw.write(p + " " + c + "\n")              
        })
        sw.close
    }

    def clusterSchema(spark: SparkSession, instanceCount: Long, dataset: String, hdfs: String) = {
        val schemaGraph = Loader.loadSchemaGraph(spark, dataset, hdfs).cache
        val schemaVertices = schemaGraph.vertices.collectAsMap

        val importanceMap = Loader.loadSchemaImportance(dataset) //load betweenness centrality
        val centroids = findCentroids(importanceMap, schemaVertices, numOfPartitions)
        var weightedSchema: Graph[String, (String, Double)] = null
        
        if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.weightedVertices + "_" + numOfPartitions) && !HdfsUtils.hdfsExists(hdfs + dataset + Constants.weightedEdges + "_" + numOfPartitions)) {
            weightedSchema = createWeightedGraph(spark, schemaGraph, importanceMap, numOfPartitions)
            val weightedVertices = weightedSchema.vertices
            val weightedEdges = weightedSchema.edges
           // Preprocessor.saveWeightedGraph(weightedVertices, weightedEdges, numOfPartitions)
        }
        else {
            weightedSchema = Loader.loadWeightedGraph(spark.sparkContext, numOfPartitions, dataset, hdfs)
        }        
        
        val nonCentroids = schemaVertices.map(_._1).toSeq

        val result = if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.shortestPaths + "_" + numOfPartitions)) {
            // weithted shortest paths for every node to every centroid
            val shortestPaths = WeightedShortestPath.run(weightedSchema, nonCentroids)
                                                .vertices
                                                .filter(x => centroids.contains(x._1))

            shortestPaths.map{case(cId, (vMap)) => (vMap.map(x => (x._1, scala.collection.immutable.Map(cId -> x._2))))}
                            .flatMap(m => m)
                            .reduceByKey(_++_)
        }
        else {
            Loader.loadShortestPaths(spark.sparkContext, numOfPartitions, dataset, hdfs)            
        }

       // if(!HdfsUtils.hdfsExists(hdfs + Constants.shortestPaths + "_" + numOfPartitions))                
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

        val clusterTriples = createClusters(schemaCluster, schemaVertices, instanceCount, dataset, hdfs)

    }

    /**
    * Creates index of class - partition
    */
    def createClassIndex(schemaCluster: Map[Long, Array[String]], centroids: Seq[Long], dataset: String) = {
        val centroidMap = centroids.zipWithIndex.toMap
        val mapWriter = new PrintWriter(new File(dataset + Constants.centroidMapFile + "_" + numOfPartitions))
        
        centroidMap.foreach{case(cId, id) => {
            mapWriter.write(cId + "," + id + "\n")
        }}
        mapWriter.close

        val pw = new PrintWriter(new File(dataset + Constants.classIndexFile + "_" + numOfPartitions + "_bal"))
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
                            schemaVertices: Map[Long, String], 
                            instanceCount: Long,
                            dataset: String, hdfs: String): Map[Long, Array[(Long, Long, String)]] = {
        var clusterCounter = Map[Long, Long]()
        var clusterNodes = Map[Long, Array[Long]]()
        var clusterTriples = Map[Long, Array[(Long, Long, String)]]()

        val centroidMap = aggregateCentroids(dependences)
        val schemaNodeCount = Loader.loadSchemaNodeCount(dataset)
        val threshold = instanceCount / numOfPartitions
        var thresholdError = threshold / numOfPartitions
        val centroids = centroidMap.keys.toSeq
        var clusteredNodes = Array[Long]()


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
            leftoutSize = if(!leftoutNodes.isEmpty){
                leftoutNodes.map(schemaVertices(_)).map(schemaNodeCount.getOrElse(_, 0)).reduce(_+_)
            }
            else {
                0
            }

            thresholdError = thresholdError + thresholdError/numOfPartitions
        }

        createClassIndex(clusterNodes.map{case(cId, nodes) => (cId, nodes.map(schemaVertices(_)))}, centroids, dataset)
        return clusterTriples
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
    * Partitions data based on partition label by using a CustomPartitioner
    */
    def partitionClusters(spark: SparkSession, finalClusters: RDD[((String, String, String), String)], dataset: String, hdfs: String) = {
        import spark.implicits._
        //val centroidMap = Loader.loadCentroiMap(numOfPartitions)
        finalClusters.map(x => (x._2, x._1))
                        .partitionBy(new CustomPartitioner(numOfPartitions))
                        .map(x => x._2)
                        .toDF().write.format("parquet").save(hdfs + dataset + Constants.clusters + "_" + numOfPartitions + "_bal")    

        val stats = finalClusters.map(x => (x._2, 1))
                                    .reduceByKey(_+_)
                                    .collect
        
        val sw = new PrintWriter(new File(dataset + Constants.statisticsFile + "_" + numOfPartitions))
        stats.foreach{case(partition, size) => {
            if(partition.toString.size == 1)
                sw.write("0"+partition.toString + " " + size + "\n")
            else
                sw.write(partition.toString + " " + size + "\n")
        }}
        sw.close
    }
    

    def subPartitionClusters(spark: SparkSession, dataset: String, hdfs: String) = {
        import spark.implicits._
        val sc = spark.sparkContext
    
        val basePath = hdfs + dataset + Constants.clusters + "_" + numOfPartitions + "_bal"
        val utilPath = dataset + Constants.clusters + "_" + numOfPartitions + "_bal"
        val partitions = HdfsUtils.getDirFilesNames(hdfs + utilPath)
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


    def fillStr(str: String) = {List.fill(5-str.length)("0").mkString + str}
    
    /**
    * Builds an index of node to class
    */
    def generateNodeIndex(spark: SparkSession, dataset: String, hdfs: String) = {
        import spark.implicits._
        val cluster = Loader.loadBaseLine(spark, dataset, hdfs)// loadClusters(spark, fullPath).distinct
        val nodeIndex = cluster.rdd.filter{case(s, o, p) => p == Constants.RDF_TYPE}
                                    .map{case(s, o, p) => {
                                        (s, o)
                                    }}
        
        val nodeIndexDf = nodeIndex.map(node => Types.NodeIndex(node._1, node._2)).toDF()
        nodeIndexDf.write.format("parquet").save(hdfs + dataset + Constants.nodeIndexFile + "_" + numOfPartitions)

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


    //compute hash of String
    def hashValue(str: String): Int = {
        var hash = 7
        for( i <- 0 to str.length()-1){
            hash = hash*31 + str.charAt(i)
        }
        hash
    }

    def roundUp(d: Double) = math.ceil(d).toInt

    
    
    def partitionInstances(sc: SparkContext, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String], dataset: String, hdfs: String): RDD[((String, String, String), String)] = {
        val classIndex = Loader.loadClassIndexBal(numOfPartitions, dataset)
        // val centroidMap = Loader.loadCentroiMap(numOfPartitions, dataset)//.map(x => (x._2, x._1))
        //val schemaCluster = Loader.loadSchemaCluster(sc, numOfPartitions).map{case(t, cId) => (t, centroidMap(cId).toString)}
        
        val brIndex = sc.broadcast(classIndex)
        // val brMap = sc.broadcast(centroidMap)

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
    def rdfCluster(spark: SparkSession, 
                    instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String], 
                    schemaGraph: Graph[String, (String, Double)], 
                    instanceCount: Long, 
                    dataset: String, hdfs: String): RDD[((String, String, String), String)] = {
        val sc = spark.sparkContext
      //  println("instance count " + instanceCount)
        //Partitioning of schema graph
        if(!new File(dataset + Constants.classIndexFile + "_" + numOfPartitions + "_bal").exists)
            clusterSchema(spark, instanceCount,dataset, hdfs)
        partitionInstances(sc, instanceGraph,dataset, hdfs)
    } 

    /**
    * add weights to the schema graph
    */
    def createWeightedGraph(spark: SparkSession,
                            schemaGraph: Graph[String, (String, Double)], 
                            importanceMap: Map[Long, Double], 
                            k: Int) = {
        val sc = spark.sparkContext
        val cc = Loader.loadCC(sc, dataset, hdfs).map(x => ((x._1, x._2, x._3),(x._4, x._5))).collectAsMap //load cardinalities CONVERT TO MAP
        val c = schemaGraph.numVertices //number of schema vertices
        val bothDirectionalSchema = Graph(schemaGraph.vertices, schemaGraph.edges.union(schemaGraph.edges.reverse))
        val brCC = sc.broadcast(cc)
        val weightedEdges= bothDirectionalSchema.triplets
                                         .map(triplet => addWeight(c, triplet, importanceMap, cc)) //create weighted graph edges 

        Graph(bothDirectionalSchema.vertices, weightedEdges) //create weighted schema graph
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