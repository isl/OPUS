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

/**
 * SemanticPartitioner
 *
 */
object LAP { 
    var subPartitionType: String = ""
    var numOfPartitions: Int = -1
    var subPartitionMode: String = ""
    var dataset: String = ""

    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
                                .appName("LAP")
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
        
        val finalClusters = rdfCluster(spark, instanceGraph, schemaGraph, numOfPartitions)

        val importanceMap = Loader.loadSchemaImportance() //load betweenness centrality
        val schemaMap = schemaGraph.vertices.collectAsMap
        val centroids = findCentroids(importanceMap, schemaMap, numOfPartitions)

  
        if(!HdfsUtils.hdfsExists(Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions)) {
            partitionClusters(spark, finalClusters)
            if(!HdfsUtils.hdfsExists(Constants.nodeIndexFile  + "_" + numOfPartitions)) {
                generateNodeIndex(spark)
            }
            subPartitionClusters(spark, subPartitionMode)
        }

        if(!new File(Constants.partitionStatFile + "_" + numOfPartitions + "_" + subPartitionMode).exists)
            computePartitionStatistics(spark)

        // if(!new File(Constants.dataStatisticsFile  + "_" + numOfPartitions).exists)
        //     computeDataStatistics(spark, numOfPartitions)

        // if(!HdfsUtils.hdfsExists(Constants.baseLine))
        //     createBaseline(spark, numOfPartitions)
        
        //createSparqlgxData(spark, dataset)
        // computePredicateReduction(spark)
        //validateIndexing(spark, instanceGraph)
        //validatePartitions(spark)
        spark.stop()
    }    

    def cleanDBPedia(spark: SparkSession) = {
        val vMap = spark.sparkContext
                        .textFile(Constants.instancePath)
                        .filter(!_.startsWith("#"))
                        .map(_.split("\\s+", 3))
                        .filter(t => t(1).trim == Constants.RDF_TYPE &&  t(0).trim.length < 150 && t(0).trim.count(_ == '%') <= 1)
                        .map(t => (t(0).trim, ""))
           
        val rdd = spark.sparkContext
                        .textFile(Constants.instancePath)
                        .filter(!_.startsWith("#"))
                        .map(_.split("\\s+", 3))
                        .map(t => (t(0).trim, t(1).trim, t(2).dropRight(2).trim))
                        .cache

        val labelRdd = rdd.filter(t => t._3.contains("\"") || t._2 == Constants.RDF_TYPE)
                            .map{case(s, p, o) => (s, (p, o))}
                            .join(vMap).map{case(s ,((p, o), d)) => (s, p, o)}
                            .distinct
                            .map(t => t._1 + "\t" + t._2 + "\t" + t._3 + " .") 

        // println(labelRdd.count)

        val subjRdd = rdd.filter(t => !t._3.contains("\"") && t._2 != Constants.RDF_TYPE)
                            .map{case(s, p, o) => (s, (p, o))}
                            .join(vMap).map{case(s, ((p, o), d)) => (s, p, o)}
                            .distinct
                            .map(t => t._1 + "\t" + t._2 + "\t" + t._3 + " .")     

        val objRdd = rdd.map{case(s, p, o) => (o, (p, s))}
                                .join(vMap).map{case(o, ((p, s), d)) => (s, p, o)}
                                .distinct
                                .map(t => t._1 + "\t" + t._2 + "\t" + t._3 + " .")     
                    
        val finalRdd = labelRdd.union(objRdd).union(subjRdd)
        println(finalRdd.count)
        finalRdd.repartition(1).saveAsTextFile(Constants.HDFS + "dbpedia/clean_instances/")
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

    def validateIndexing(spark: SparkSession, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String]) = {
        instanceGraph.vertices.filter(_._2._2.contains("http://www.w3.org/2002/07/owl#Ontology")).map(_._2._1).distinct
                        .collect.foreach(println)
        import spark.implicits._
        println("Vertices out of indexes")
        val indexedVertices = Loader.loadNodeIndex(spark, numOfPartitions).map(_.uri).rdd
        val vertices = instanceGraph.vertices.map(_._2._1).filter(x => !x.contains("\"") && !x.startsWith("_:")) 
        val sub = vertices.subtract(indexedVertices).distinct
        println("count of vertices - non literal/blank nodes " + sub.count)

        val untyped = instanceGraph.vertices
                        .filter(x => x._2._2.isEmpty || 
                                (!x._2._2.contains("http://www.w3.org/2002/07/owl#Thing") && x._2._2.size == 1) || 
                                (!x._2._2.contains("http://xmlns.com/foaf/0.1/OnlineAccount") && x._2._2.size == 1))
                        .map(_._2._1)
                        .distinct
        
        println("count of vertices that has a type " + sub.subtract(untyped).count)
        println("types of vertices that are not indexed")

        sub.subtract(untyped).map(x => (x, 1))
                                .join(instanceGraph.vertices.map(x => (x._2._1, x._2._2)))
                                .flatMap(x => x._2._2)
                                .distinct.collect.foreach(println)

        println("not indexed vertices with type")
        sub.subtract(untyped).collect.foreach(println)
    }

    /**
    * Validates data between original file and partitioned triples
    */
    def validatePartitions(spark: SparkSession) = {
        val rdd = spark.sparkContext.textFile(Constants.instancePath)
                        .filter(!_.startsWith("#"))
                        .map(_.split("\\s+", 3))
                        .map(t => (t(0).trim, t(1).trim, t(2).dropRight(2).trim))
                        .distinct//.cache

        val clusters = Loader.loadClusters(spark, Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "/*.parquet")
                            .distinct.rdd.map{case(s, o, p) => (s, p, o)}
//                            .cache

        val leftout = rdd.subtract(clusters)

        // println(clusters.count)
        // println(rdd.count)
        // val pw = new PrintWriter(new File("partitionLeftOut.txt"))
    
        // println("left out rdf types " + leftout.filter(_._3 == Constants.RDF_TYPE).count)
        // leftout.collect.foreach{case(s, o, p) => {
        // //     pw.write(s + "\t" + p + "\t" + o + " .\n")
        // // }}
        // // pw.close
        clusters.subtract(rdd).collect.foreach(println)
        // println("leftout triples " + clusters.subtract(rdd).count)
    }

    /**
    * Validates data between original file and graph creation
    */
    def validateData(spark: SparkSession, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String]) = {
        val rdd = spark.sparkContext.textFile(Constants.instancePath)
                        .filter(!_.startsWith("#"))
                        .map(_.split("\\s+", 3))
                        .map(t => (t(0).trim, t(1).trim, t(2).dropRight(2).trim))
                        .distinct

        val cleanRdd = spark.sparkContext.textFile(Constants.HDFS + "dbpedia/clean_instances/part-00000")
                        .filter(!_.startsWith("#"))
                        .map(_.split("\\s+", 3))
                        .map(t => (t(0).trim, t(1).trim, t(2).dropRight(2).trim))
                        
        
        println(cleanRdd.subtract(rdd).count)
        System.exit(-1)
        val instanceTriples = instanceGraph.triplets.map{case(e) => (e.srcAttr._1, e.dstAttr._1, e.attr)}


        println(rdd.subtract(instanceTriples).count)
        println(instanceTriples.subtract(rdd).count)
        println(instanceTriples.subtract(rdd).filter(_._2.contains("\"")).count)
        println("\n\n")

        val pw = new PrintWriter(new File("leftOutTriples.txt"))
        val leftout = rdd.subtract(instanceTriples)
        println("left out rdf types " + leftout.filter(_._3 == Constants.RDF_TYPE).count)
        leftout.collect.foreach{case(s, o, p) => {
            pw.write(s + "\t" + p + "\t" + o + " .\n")
        }}
        pw.close
    }

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
            val exists = HdfsUtils.hdfsExists(Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "/" + suffix +"/01")
            if(exists) {
                val subPartitions = HdfsUtils.countSubFolders(Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "/" + suffix)
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
        val path = Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + k + "/*.parquet"
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
                        .toDF().write.format("parquet").save(Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions)    

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
    
        val basePath = Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions
        val utilPath = Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions
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
            val basePath = Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "/"
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
        val basePath = Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "/"
        val baseUtilsPath = Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "/"
        val sw = new PrintWriter(new File(Constants.partitionStatFile + "_" + numOfPartitions + "_" + subPartitionMode))
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
        val basePath = Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "/"
        val baseUtilsPath = Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "/"
        val sw = new PrintWriter(new File(Constants.dataStatisticsFile  + "_" + numOfPartitions + "_" + subPartitionMode))
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

    /**
    * Creates index of class - partition
    */
    def createClassIndex(schemaClusters: RDD[(Long, (String, String, String))], centroids: Seq[Long], emptyCentroids: Array[(Long, String)]) = {
        val centroidMap = centroids.zipWithIndex.toMap
        val mapWriter = new PrintWriter(new File(Constants.centroidMapFile + "_" + numOfPartitions))
        
        centroidMap.foreach{case(cId, id) => {
            mapWriter.write(cId + "," + id + "\n")
        }}
        mapWriter.close

        val classIndex = schemaClusters.flatMap{case(cId, (s, o, p)) => 
                                                    Array((s, Array(centroidMap(cId))), (o, Array(centroidMap(cId))))
                                                }
                                        .reduceByKey(_++_)  //index empty centroids
                                        .collect.toArray ++ emptyCentroids.map{case(id, uri) => (uri, Array(centroidMap(id)))}

        val pw = new PrintWriter(new File(Constants.classIndexFile + "_" + numOfPartitions))
        var i = 0
        while(i < classIndex.length) {
            pw.write(classIndex(i)._1 + "\t" + classIndex(i)._2.distinct.mkString(",") + "\n")
            i+=1
        }
        pw.close
    }

    /**
    * SubPartitions clases based on class index and sub partition map
    */
    def indexSubPartitions(subPartitionMap: Map[String, Int]) = {
        val classIndex = Loader.loadClassIndex(numOfPartitions).map{case(c, partitions) => (c, partitions.map(fillStr(_)))}
        var partitionClasses = Array[(String, String)]()
        classIndex.foreach{case(c, partitions) => {
            partitions.foreach(p => partitionClasses = partitionClasses :+ (p, c))
        }}
        val partitionArray: Array[(String, Array[String])] = partitionClasses.groupBy(_._1).map(tuple => (tuple._1, tuple._2.map(_._2))).toArray
        
        partitionClasses = Array[(String, String)]()
        partitionArray.foreach{case(p, classes) => {
            val splitSize = subPartitionMap(p)
            val chunkSize = classes.size / splitSize.toDouble
            var curSubPartition = 0
            var count = 0
            if(splitSize == 0) {
                partitionClasses = partitionClasses ++ classes.map(c => (c, p))
            }
            else {
                partitionClasses = partitionClasses ++ classes.map{c => {
                    if(count < chunkSize){
                        count+=1
                        (c, p + "-" + fillStr(curSubPartition.toString))
                    }
                    else {
                        count = 1
                        curSubPartition+=1
                        (c, p + "-" + fillStr(curSubPartition.toString))
                    }
                }}
            }          
        }}
        val finalClassIndex = partitionClasses.groupBy(_._1).map(tuple => (tuple._1, tuple._2.map(_._2)))
        
        finalClassIndex.foreach{case(c, partitions) => if(!QueryParser.arrayEquals(classIndex(c), partitions.map(_.split("-")(0)))) println("Error in index creation")}

        val pw = new PrintWriter(new File(Constants.finalClassIndexFile + "_" + numOfPartitions + "_" + subPartitionMode))
        finalClassIndex.foreach{case(c, partitions) => {
            pw.write(c + "\t" + partitions.distinct.mkString(",") + "\n")
        }}
        pw.close
    }


    def fillStr(str: String) = {List.fill(5-str.length)("0").mkString + str}
    
    /**
    * Builds an index of node to class
    */
    def generateNodeIndex(spark: SparkSession) = {
        import spark.implicits._
        val fullPath = Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + numOfPartitions + "/"
        val cluster = Loader.loadClusters(spark, fullPath).distinct
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

    /**
    * Create index for nodes and subclasses
    * **Probably no use of this function**
    */
    def findSubClassRelationships(schemaGraph: Graph[String, (String, Double)]): Array[Array[Long]] = {
        val subClassTriples = schemaGraph.triplets
                                            .filter(t => t.attr._1 == "http://www.w3.org/2000/01/rdf-schema#subClassOf")
                                            .cache
        
        val subClassEdges =  subClassTriples.map(t => Edge(t.srcId, t.dstId, t.attr._1))
    
        val subClassVertices = subClassTriples.flatMap(t => Seq((t.srcId, t.srcAttr), (t.dstId, t.dstAttr)))
        subClassTriples.unpersist()

        val subClassGraph = Graph(subClassVertices, subClassEdges)

        val landMarks = subClassVertices.map(_._1).collect

        val sp = ShortestPaths.run(subClassGraph, landMarks)

        val vertexMap = subClassGraph.vertices.collectAsMap

        sp.vertices.filter(v => v._2.size > 1)
                    .map(v => v._2.keys.toArray)        
                    .collect
    }
    // def partitionInstances(sc: SparkContext, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String]): RDD[((String, String, String), String)] = {
    //     val classIndex = Loader.loadClassIndex(numOfPartitions)
    //     val brIndex = sc.broadcast(classIndex)
    //     //map each vertex classes to its ids
    //     val pGraph = instanceGraph.triplets.flatMap(t => populateTriples(t.srcAttr, t.dstAttr, t.attr))
    //                                         .distinct
    //                                         .map{case(triple, p) => (brIndex.value(p).map(p => (triple, p)))}
    //                                         .flatMap(triples => triples)
    //                                         .distinct
    //     pGraph
        
    // }   

    // def populateTriples(srcAttr: (String, Seq[String], Seq[(String, String)]), 
    //                     dstAttr: (String, Seq[String], Seq[(String, String)]), 
    //                     attr: String): Seq[((String, String, String), String)] = {
    //     val srcUri = srcAttr._1
    //     val srcTypes = srcAttr._2//.map(getUriLastPart(_))
    //     val srcLabels = srcAttr._3
        
    //     val dstUri = dstAttr._1
    //     val dstTypes = dstAttr._2//.map(getUriLastPart(_))
    //     val dstLabels = dstAttr._3

    //     val pTriple = (srcTypes ++ dstTypes).map(t => ((srcUri, dstUri, attr), t))
    //     val pSrcLabels = if(!srcLabels.isEmpty){
    //         srcLabels.map{case(p, o) => (srcUri, o, p)}
    //                     .map(t => (srcTypes.map(srcType => (t, srcType))))
    //                     .reduce((a, b) => a++b)
    //     } else {
    //         Seq()
    //     }
        
    //     val pDstLabels = if(!dstLabels.isEmpty){
    //         dstLabels.map{case(p, o) => (dstUri, o, p)}
    //                     .map(t => (dstTypes.map(srcType => (t, srcType))))
    //                     .reduce((a, b) => a++b)
    //     } else {
    //         Seq()
    //     }

    //     val pSrcTypes = srcTypes.map(srcType => ((srcUri, srcType, Constants.RDF_TYPE), srcType))
    //     val pDstTypes = dstTypes.map(dstType => ((dstUri, dstType, Constants.RDF_TYPE), dstType))

    //     (pTriple ++  pSrcLabels ++ pDstLabels ++ pSrcTypes ++ pDstTypes).distinct
    // }
    
    def partitionInstances(sc: SparkContext, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String]): RDD[((String, String, String), String)] = {
        val classIndex = Loader.loadClassIndex(numOfPartitions)
        val centroidMap = Loader.loadCentroiMap(numOfPartitions)//.map(x => (x._2, x._1))
        val schemaCluster = Loader.loadSchemaCluster(sc, numOfPartitions).map{case(t, cId) => (t, centroidMap(cId).toString)}
        
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
        if(!HdfsUtils.hdfsExists(Constants.schemaClusterFile + "_" + k))
            clusterSchema(spark, schemaGraph, k)
        
        partitionInstances(sc, instanceGraph)
    }


    /**
    * Partitions schema graph
    */
    def clusterSchema(spark: SparkSession, schemaGraph: Graph[String, (String, Double)], k: Int) = {
        val sc = spark.sparkContext
        val importanceMap = Loader.loadSchemaImportance() //load betweenness centrality
        var weightedSchema: Graph[String, (String, Double)] = null
        if(!HdfsUtils.hdfsExists(Constants.weightedVertices + "_" + k) && !HdfsUtils.hdfsExists(Constants.weightedEdges + "_" + k)) {
            weightedSchema = createWeightedGraph(spark, schemaGraph, importanceMap, k)
            val weightedVertices = weightedSchema.vertices
            val weightedEdges = weightedSchema.edges
          //  Preprocessor.saveWeightedGraph(weightedVertices, weightedEdges, k)
        }
        else {
            weightedSchema = Loader.loadWeightedGraph(sc, k)
        }

        val schemaVertices = schemaGraph.vertices.collectAsMap
        val centroids = findCentroids(importanceMap, schemaVertices, k)
        val nonCentroids = schemaVertices.map(_._1).toSeq
        weightedSchema.edges.repartition(50).cache
        weightedSchema.vertices.repartition(6).cache

        val result = if(!HdfsUtils.hdfsExists(Constants.shortestPaths + "_" + k)) {
            // weithted shortest paths for every node to every centroid
            val shortestPaths = WeightedShortestPath.run(weightedSchema, nonCentroids)
                                                .vertices
                                                .filter(x => centroids.contains(x._1))

            shortestPaths.map{case(cId, (vMap)) => (vMap.map(x => (x._1, scala.collection.immutable.Map(cId -> x._2))))}
                            .flatMap(m => m)
                            .reduceByKey(_++_)
        }
        else {
            Loader.loadShortestPaths(sc, k)            
        }

        // if(!HdfsUtils.hdfsExists(Constants.shortestPaths + "_" + k))                
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
        finalSchemaCluster.saveAsTextFile(Constants.HDFS + Constants.schemaClusterFile + "_" + k)
        // createClassIndex(finalSchemaCluster.map(x => (x._2, x._1)), centroids, Array())
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