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
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import sys.process._

/**
 * SemanticPartitioner
 *
 */
object LAP { 
    var numOfPartitions: Int = -1
    var dataset: String = ""
    var hdfs: String = ""

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
                                .appName("LAP")
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
        if(!hdfs.endsWith("/"))
            hdfs = hdfs + "/"

        val instancePath = args(3)
        val schemaPath = args(4)

        val mkdir = "mkdir " + dataset + "_local"
        val mkdirOut = mkdir ! 

        Preprocessor.initializeGraphs(spark, dataset, hdfs, schemaPath, instancePath)

        val schemaGraph = Loader.loadSchemaGraph(spark, dataset, hdfs).cache
        
        val instanceGraph = Loader.loadInstanceGraph(spark, dataset, hdfs)
                                      .partitionBy(PartitionStrategy.EdgePartition2D, 1600)

        Preprocessor.computeMeasures(sc, instanceGraph, schemaGraph, dataset, hdfs)
        
        val finalClusters = rdfCluster(spark, instanceGraph, schemaGraph, numOfPartitions)
  
        if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.clusters + "_" + numOfPartitions)) {
            partitionClusters(spark, finalClusters)
            if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.nodeIndexFile  + "_" + numOfPartitions)) {
                generateNodeIndex(spark)
            }
            subPartitionClusters(spark)
        }

        if(!new File(dataset + Constants.partitionStatFile + "_" + numOfPartitions).exists)
            computePartitionStatistics(spark)

        spark.stop()
    }    
                 

    def computePartitionStatistics(spark: SparkSession) = {
        val basePath = hdfs + dataset + Constants.clusters + "_" + numOfPartitions + "/"
        val baseUtilsPath = dataset + Constants.clusters + "_" + numOfPartitions + "/"
        val sw = new PrintWriter(new File(dataset + Constants.partitionStatFile + "_" + numOfPartitions ))
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
    * Partitions data based on partition label by using a CustomPartitioner
    */
    def partitionClusters(spark: SparkSession, finalClusters: RDD[((String, String, String), String)]) = {
        import spark.implicits._
        //val centroidMap = Loader.loadCentroiMap(numOfPartitions)
        finalClusters.map(x => (x._2, x._1))
                        .partitionBy(new CustomPartitioner(numOfPartitions))
                        .map(x => x._2)
                        .toDF().write.format("parquet").save(hdfs + dataset + Constants.clusters + "_" + numOfPartitions)    

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

    def subPartitionClusters(spark: SparkSession) = {
        import spark.implicits._
        val sc = spark.sparkContext
    
        val basePath = hdfs + dataset + Constants.clusters + "_" + numOfPartitions
        val utilPath = dataset + Constants.clusters + "_" + numOfPartitions
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

            val partSize = partition.rdd.partitions.size
            
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
    * Creates index of class - partition
    */
    def createClassIndex(schemaClusters: RDD[(Long, (String, String, String))], centroids: Seq[Long], emptyCentroids: Array[(Long, String)]) = {
        val centroidMap = centroids.zipWithIndex.toMap
        val mapWriter = new PrintWriter(new File(dataset + Constants.centroidMapFile + "_" + numOfPartitions))
        
        centroidMap.foreach{case(cId, id) => {
            mapWriter.write(cId + "," + id + "\n")
        }}
        mapWriter.close

        val classIndex = schemaClusters.flatMap{case(cId, (s, o, p)) => 
                                                    Array((s, Array(centroidMap(cId))), (o, Array(centroidMap(cId))))
                                                }
                                        .reduceByKey(_++_)  //index empty centroids
                                        .collect.toArray ++ emptyCentroids.map{case(id, uri) => (uri, Array(centroidMap(id)))}

        val pw = new PrintWriter(new File(dataset + Constants.classIndexFile + "_" + numOfPartitions))
        var i = 0
        while(i < classIndex.length) {
            pw.write(classIndex(i)._1 + "\t" + classIndex(i)._2.distinct.mkString(",") + "\n")
            i+=1
        }
        pw.close
    }

        
    /**
    * Builds an index of node to class
    */
    def generateNodeIndex(spark: SparkSession) = {
        import spark.implicits._
        val fullPath = hdfs + dataset + Constants.clusters + "_"+ numOfPartitions + "/"
        val cluster = Loader.loadClusters(spark, fullPath).distinct
        val nodeIndex = cluster.rdd.filter{case(s, o, p) => p == Constants.RDF_TYPE}
                                    .map{case(s, o, p) => {
                                        (s, o)
                                    }}
        
        val nodeIndexDf = nodeIndex.map(node => Types.NodeIndex(node._1, node._2)).toDF()
        nodeIndexDf.write.format("parquet").save(hdfs + dataset +Constants.nodeIndexFile + "_" + numOfPartitions)
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

    def roundUp(d: Double) = math.ceil(d).toInt
   
    
    def partitionInstances(sc: SparkContext, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String]): RDD[((String, String, String), String)] = {
        val classIndex = Loader.loadClassIndex(numOfPartitions, dataset)
        val centroidMap = Loader.loadCentroiMap(numOfPartitions, dataset)//.map(x => (x._2, x._1))
        val schemaCluster = Loader.loadSchemaCluster(sc, numOfPartitions, dataset, hdfs).map{case(t, cId) => (t, centroidMap(cId).toString)}
        
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

        val finalClusters = schemaCluster.union(partitionTypeTriples).union(partitionedInstances).union(labelTriples).distinct
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
    def rdfCluster(spark: SparkSession, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String], schemaGraph: Graph[String, (String, Double)], k: Int): RDD[((String, String, String), String)] = {
        val sc = spark.sparkContext
        
        //Partitioning of schema graph
        if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.schemaClusterFile + "_" + k))
            clusterSchema(spark, schemaGraph, k)
        
        partitionInstances(sc, instanceGraph)
    }


    /**
    * Partitions schema graph
    */
    def clusterSchema(spark: SparkSession, schemaGraph: Graph[String, (String, Double)], k: Int) = {
        val sc = spark.sparkContext
        val importanceMap = Loader.loadSchemaImportance(dataset) //load betweenness centrality
        var weightedSchema: Graph[String, (String, Double)] = null
        if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.weightedVertices + "_" + k) && !HdfsUtils.hdfsExists(hdfs + dataset + Constants.weightedEdges + "_" + k)) {
            weightedSchema = createWeightedGraph(spark, schemaGraph, importanceMap, k)
            val weightedVertices = weightedSchema.vertices
            val weightedEdges = weightedSchema.edges
          //  Preprocessor.saveWeightedGraph(weightedVertices, weightedEdges, k)
        }
        else {
            weightedSchema = Loader.loadWeightedGraph(sc, numOfPartitions, dataset, hdfs)
        }

        val schemaVertices = schemaGraph.vertices.collectAsMap
        val centroids = findCentroids(importanceMap, schemaVertices, k)
        val nonCentroids = schemaVertices.map(_._1).toSeq
        weightedSchema.edges.repartition(50).cache
        weightedSchema.vertices.repartition(6).cache

        val result = if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.shortestPaths + "_" + k)) {
            // weithted shortest paths for every node to every centroid
            val shortestPaths = WeightedShortestPath.run(weightedSchema, nonCentroids)
                                                .vertices
                                                .filter(x => centroids.contains(x._1))

            shortestPaths.map{case(cId, (vMap)) => (vMap.map(x => (x._1, scala.collection.immutable.Map(cId -> x._2))))}
                            .flatMap(m => m)
                            .reduceByKey(_++_)
        }
        else {
            Loader.loadShortestPaths(sc, k, dataset, hdfs)            
        }

        // if(!HdfsUtils.hdfsExists(hdfs + Constants.shortestPaths + "_" + k))                
        //     Preprocessor.saveShortestPathVertices(result, k)
        
        //Array with schema edges
        val schemaEdges = schemaGraph.triplets
                                        .map(t => (t.srcAttr, t.dstAttr, t.attr._1))
                                        .distinct
                                        .collect
        //Map with centroids [id, uri]
        val centroidMap = schemaGraph.vertices
                                        .filter(v => centroids.contains(v._1))
                                        .collectAsMap        

        val diffs = result.filter(node => !node._2.isEmpty)
                            .flatMap(node => dependenceDiff(importanceMap, node._2))

        val maxDiff = diffs.max
        val minDiff = diffs.min

        //Maps every triple with the coresponding centroid
        val schemaClusters = result.filter(node => !node._2.isEmpty)
                                    .map(node => dependence(importanceMap, node._2, maxDiff, minDiff))
                                    .flatMap(node => mapIdToTriples(node.maxBy(_._2._1)))
        result.unpersist()

        //replace every reverse triple with the original one
        val validSchemaClusters = schemaClusters.map{case(cId, triple) => {
                                                            if(schemaEdges.contains(triple))
                                                                (cId, replaceVertexIds(triple, schemaVertices))
                                                            else
                                                                (cId, replaceVertexIds((triple._2, triple._1, triple._3), schemaVertices))
                                                    }} 
        //Hack to index centroids that are empty
        val emptyCentroidMap = centroids.diff(validSchemaClusters.map(_._2).distinct.collect)
                                        .map(cId => (cId, schemaVertices(cId)))
                                        .toArray
                                        
        createClassIndex(validSchemaClusters, centroids, emptyCentroidMap)

        //map each node in the cluster with every triple it is connected
        val extendedSchemaCluster = validSchemaClusters.map{case(cId, curEdge) => 
                                                                        extendCluster(cId, 
                                                                                        curEdge, 
                                                                                        schemaEdges)
                                                                    }
                                                        .flatMap(list => list)
                                                        .distinct

        val emptyCentroids = centroids.diff(extendedSchemaCluster.map(_._2).distinct.collect)
        val extendedCentroids = sc.parallelize(emptyCentroids.map(cId => extendCentroid(cId, schemaVertices(cId), schemaEdges)))                                                
                                    .flatMap(list => list)
                                    .distinct

        val finalSchemaCluster = extendedSchemaCluster.union(extendedCentroids).distinct
        finalSchemaCluster.saveAsTextFile(hdfs + dataset + Constants.schemaClusterFile + "_" + k)
        // createClassIndex(finalSchemaCluster.map(x => (x._2, x._1)), centroids, Array())
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
        val cc = Loader.loadCC(sc, dataset  , hdfs).map(x => ((x._1, x._2, x._3),(x._4, x._5))).collectAsMap //load cardinalities CONVERT TO MAP
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
    def dependence(importanceMap: Map[Long, Double], centroidMap: Map[Long, Tuple2[Double, Seq[Tuple3[Long, Long, String]]]], max: Double, min: Double) = {
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