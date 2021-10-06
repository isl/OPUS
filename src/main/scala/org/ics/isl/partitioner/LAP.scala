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

//import sqlContext.implicits._
//import org.apache.spark.sql.functions._
//import org.slf4j.LoggerFactory
//val logger = LoggerFactory.getLogger(getClass.getSimpleName)
//  logger.info("I am a log message")

//import com.typesafe.scalalogging.Logger
//val logger = Logger("Root")
//logger.info("Hello there!")

/**
 * SemanticPartitioner
 *
 */
object LAP {
    var subPartitionType: String = ""
    var numOfPartitions: Int = -1
    var dataset: String = ""
    var hdfs = ""
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder
                                .appName("LAP")
                                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                .config("spark.kryoserializer.buffer.max", "1536m")
                                .config("spark.driver.maxResultSize", "10G")
                                .config("spark.sql.inMemoryColumnarStorage.compressed", "true") //s2rdf
                                .config("spark.hadoop.dfs.replication", "1")
                                .getOrCreate()
        import spark.implicits._
        
        val sc = spark.sparkContext

        if(args.size < 4){
            println("Missing Arguments")
            println("Arguments: dataset name, hdfs base path, insances path, schema path")
            System.exit(-1)
        }
        
        //numOfPartitions = args(0).toInt
        dataset = args(0)
        hdfs = args(1)

      dataset match {
        case "swdf" => numOfPartitions = 4
        case "lubm"  => numOfPartitions = 8
        case "lubm8"  => numOfPartitions = 12
        case "dbpedia"  => numOfPartitions = 8
        case _ => numOfPartitions = 4
      }

        val instancePath = args(2)
        val schemaPath = args(3)
        val mkdir = "mkdir " + dataset + "_local"

        if(!hdfs.endsWith("/"))
            hdfs = hdfs + "/"

        sc.setCheckpointDir(hdfs + "checkPoint")
        sc.setLogLevel("ERROR")

        var timeStart = System.currentTimeMillis;

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
                //!! generateNodeIndex(spark, dataset, hdfs)
            }
            subPartitionClusters(spark, dataset, hdfs, numOfPartitions)
        }
        if(!new File(dataset + Constants.partitionStatFile + "_" + numOfPartitions + "_bal").exists)
            computePartitionStatistics(spark)

      var timeLAP = System.currentTimeMillis - timeStart;
      println("LAP - partitioning time: " + timeLAP + "ms");
      spark.stop()
    }    

    def computePartitionStatistics(spark: SparkSession) = {
        val basePath = hdfs + dataset + Constants.clusters + "_" + numOfPartitions + "_bal/"
        val baseUtilsPath = dataset + Constants.clusters + "_" + numOfPartitions + "_bal/"
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
        val schemaVertices = schemaGraph.vertices.collectAsMap // (e.id, e.uri)

        val importanceMap = Loader.loadSchemaImportance(dataset) //(id_vertices, value), load betweenness centrality  -- file:  dataset_local/schema_node_importance.txt
        val centroids = findCentroids(importanceMap, schemaVertices, numOfPartitions)
        //centroids.foreach(c => println("CENTROID:"+c))
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
        
        val nonCentroids = schemaVertices.map(_._1).toSeq //take id of vertices -- all vertices

        val result = if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.shortestPaths + "_" + numOfPartitions)) {
            // weithted shortest paths for every node to every centroid
            val shortestPaths = WeightedShortestPath.run(weightedSchema, nonCentroids)
                                                .vertices
                                                .filter(x => centroids.contains(x._1))

            shortestPaths.map{case(cId, (vMap)) => (vMap.map(x => (x._1, scala.collection.immutable.Map(cId -> x._2))))} //cId: centroids  x_1:node
                            .flatMap(m => m)
                            .reduceByKey(_++_)
        }
        else {
          println("ShortestPaths exist!")
          Loader.loadShortestPaths(spark.sparkContext, numOfPartitions, dataset, hdfs)
        }

         // if(!HdfsUtils.hdfsExists(hdfs + Constants.shortestPaths + "_" + numOfPartitions))
         // Preprocessor.saveShortestPathVertices(result, numOfPartitions)
        // val result = Loader.loadShortestPaths(spark.sparkContext, numOfPartitions)         
        
        val diffs = result.filter(node => !node._2.isEmpty)
                            .flatMap(node => dependenceDiff(importanceMap, node._2))

        val maxDiff = diffs.max
        val minDiff = diffs.min

        //result: case(node_centoid_id, dependenceMap)
        //depedence() return: [CentroidId, Dependence]
        //node._1: node_id (no centroid!)
        //node._2: (centroid_id, [depednece_path_value, Seq(id_dest, id_source, namePredicate)]) == Dependence(?)

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
    //(node \t id_centroid1, id_centroid2,..)
    def createClassIndex(schemaCluster: Map[Long, Array[String]], centroids: Seq[Long], dataset: String) = {
        val centroidMap = centroids.zipWithIndex.toMap
        val mapWriter = new PrintWriter(new File(dataset + Constants.centroidMapFile + "_" + numOfPartitions))

        centroidMap.foreach{case(cId, id) => { //cId:centroid
            mapWriter.write(cId + "," + id + "\n")
        }}
        mapWriter.close
       // println("---------------  createClassIndex:"+dataset + Constants.classIndexFile + "_" + numOfPartitions + "_bal")
        val pw = new PrintWriter(new File(dataset + Constants.classIndexFile + "_" + numOfPartitions + "_bal"))
        var classIndex = Map[String, Array[Long]]()
        //schemaCluster.foreach{case(cId, nodes) => println(cId+ " << >> "+nodes.length)}
        //schemaCluster.foreach{case(cId, nodes) => println(cId+ " << >> "+nodes.mkString(","))}

        //add mode-centroid to the list of nodes located to specific centroid
        schemaCluster.foreach{case(cId, nodes) => {
            nodes.foreach(n => {
                if(classIndex.contains(n)){
                    classIndex = classIndex + (n -> (classIndex(n) :+ cId))
                }
                else {
                    classIndex = classIndex + (n -> Array(cId))   
                }
            })
        }} //(nodes, Array(centroids_cId))
        classIndex.foreach{case(node, cIds) => {
            pw.write(node + "\t" + cIds.map(centroidMap(_)).distinct.mkString(",") + "\n")
        }} //(node_class \t id_centroid1, id_centroid2,..) not cID but id (partition: 0, 1, 2,...)!
        pw.close
    }

  //(node =>(node._1, dependence(importanceMap, node._2, maxDiff, minDiff)))
    def createClusters(dependences: Seq[(Long, Map[Long, (Double, Seq[(Long, Long, String)])])],
                            schemaVertices: Map[Long, String], 
                            instanceCount: Long,
                            dataset: String, hdfs: String): Map[Long, Array[(Long, Long, String)]] = {
        var clusterCounter = Map[Long, Long]()
        var clusterNodes = Map[Long, Array[Long]]()
        var clusterTriples = Map[Long, Array[(Long, Long, String)]]()

        //untill now: (node_id, DepMap) --dependences: (cId_centroid, (cId2, (dep, path))) -- path: (Long, Long, String)
        val centroidMap = aggregateCentroids(dependences)  ////now: (cID, DepMap) --  DepMap: (ordering) Array[(dep, node_id, Seq(path)), ..]  // centroidMap.map{case(cId, dependences) => (cId, dependences.sortBy(_._1)(Ordering[Double].reverse))}
        val schemaNodeCount = Loader.loadSchemaNodeCount(dataset)
        val threshold = instanceCount / numOfPartitions
        var thresholdError = threshold / numOfPartitions
        val centroids = centroidMap.keys.toSeq
        var clusteredNodes = Array[Long]()

        // val test1 = dependences.map{case(node, dep) => (node, dep.map(d => (d._1)))}
        //test1.foreach{case(node, cids) => println("~~" +node+ ": "+cids.mkString(","))}

       //Depdneces : centroidMap.foreach{case(cID, dependences) => {dependences.foreach(d => println(cID + " ~ "+d._2+": "+d._1) )}}

      //todo: val test = centroidMap.map{case(cID, dependences) => (cID, dependences.map(d => d._2) )}
      //test.foreach{case(cID, nodes) => println("-- "+cID +": "+ nodes.mkString(","))}

      val numNodes = centroidMap.map{case(cID, dependences) => dependences.length}.min
      val numNodesMax = centroidMap.map{case(cID, dependences) => dependences.length}.max
      println("SIZE max : "+numNodesMax+ " - min:" + numNodes)

      //centroidMap.foreach{case(cID, dependences) => println(cID + ">>>  ")}
      //initialize maps
      centroids.foreach(c => {
            clusterCounter = clusterCounter + (c -> 0)
            clusterNodes = clusterNodes + (c -> Array[Long]())
            clusterTriples = clusterTriples + (c -> Array[(Long, Long, String)]())
      })

      val array = Array("<http://www.w3.org/2004/02/skos/core#Concept>", "<http://www.w3.org/2002/07/owl#Class>", "<Version 3.8>", "<http://dbpedia.org/ontology/>")
      val intialValidNodes = schemaVertices.keys.toSet.filter(x => !centroids.contains(x) && !array.contains(x)) //all nodes except centroids and invalid
      var leftoutNodes = intialValidNodes.toSet //all nodes excpet form centroids
      var leftoutSize = leftoutNodes.map(schemaVertices(_)).map(schemaNodeCount.getOrElse(_, 0)).reduce(_+_)


        while(leftoutSize != 0) {
          for{i <- 0 to numNodes - 1  if leftoutSize != 0}{
            centroidMap.foreach { case (cId, dependences) => { //dependences: (cId_centroid, (cId2, (dep, path)))
              val node = dependences(i)._2
              val path = dependences(i)._3
              breakable {
                //if node is clustered continue
                if (clusteredNodes.contains(node) || array.contains(node))
                  break
                //todo: if currCnentroid is empty add the centroid node in  - nonoe centroid empty
                //current centrois nodes
                val curCentroidNodes = clusterNodes.getOrElse(cId, Array()) //(c -> Array[Long]())
                //find nodes that current centroid does not contain
                val newNodes: Seq[Long] = path.map { case (src, dst, pred) => Array(src, dst) }
                  .flatten.distinct
                  .filter(x => !curCentroidNodes.contains(x))
                val newNodesize = if (!newNodes.isEmpty) { //estimate the sum of instances of the added nodes
                  newNodes.map(n => schemaNodeCount.getOrElse(schemaVertices(n), 0))
                    .reduce(_ + _)
                }
                else { //no nodes to enter
                  clusteredNodes = clusteredNodes :+ node
                  break //continue
                }

                val curClusterSize = clusterCounter(cId)
                val num = curClusterSize + newNodesize < threshold

                //if incoming nodes fit into cluster OR cluster is empty
                if (curClusterSize + newNodesize < threshold || curClusterSize == 0) {
                  clusterNodes = clusterNodes + (cId -> (clusterNodes(cId) ++ newNodes)) //newNodes: Seq[Long]
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
            }
          }

          leftoutNodes = leftoutNodes -- clusteredNodes.toSet
          leftoutSize = if (!leftoutNodes.isEmpty) {
            leftoutNodes.map(schemaVertices(_)).map(schemaNodeCount.getOrElse(_, 0)).reduce(_ + _)
          }
          else {
            0
          }
        }

          thresholdError = thresholdError + thresholdError/numOfPartitions
        } //end:  while(leftoutSize != 0)

        //clusterNodes = clusterNodes + (c -> Array[Long]())
        val  clusterClasses = clusterNodes.map{case(cId, nodes) => (cId, nodes.map(schemaVertices(_)))}//.map{case(cId, nodes) =>(cId, nodes :+ schemaVertices(cId))}
        //if some centroids are empty
        val updatedClusterClasses  = clusterClasses.map{case(cId, schemaNodes) => if(schemaNodes.size == 0) (cId, schemaNodes :+ schemaVertices(cId)) else (cId, schemaNodes)}

        createClassIndex(updatedClusterClasses, centroids, dataset) //clusterNodes(cID, [String1, String2..])
        return clusterTriples
    }
      //clusterNodes(cId).length: classes in the cluster
      //clusterCounter(cId): instances in the cluster

    def aggregateCentroids(dependences: Seq[(Long, Map[Long, (Double, Seq[(Long, Long, String)])])]):
                                        Map[Long, Array[(Double, Long, Seq[(Long, Long, String)])]] = {
        var centroidMap = Map[Long, Array[(Double, Long, Seq[(Long, Long, String)])]]()
        dependences.foreach{case(node, depMap) => { //dependences: (node, (cId2, (dep, path)))
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
        centroidMap.map{case(cId, dependences) => (cId, dependences.sortBy(_._1)(Ordering[Double].reverse))} //now: (cID, DepMap) --  DepMap: (dep, node_id, Seq(path))

    }

    /**
    * Partitions data based on partition label by using a CustomPartitioner
    */
      //create partitioned files -- delete them in the next steps!
    def partitionClusters(spark: SparkSession, finalClusters: RDD[((String, String, String), String)], dataset: String, hdfs: String) = {
        import spark.implicits._
        //val centroidMap = Loader.loadCentroiMap(numOfPartitions)

        finalClusters.map(x => (x._2, x._1)) //[(centroid_id, triple)]
                        .partitionBy(new CustomPartitioner(numOfPartitions)) //take first element of each tuple - centrooid_ID
                        .map(x => x._2) //x._2:triple
                        .toDF().write.format("parquet").save(hdfs + dataset + Constants.clusters + "_" + numOfPartitions + "_bal")    //create files for each centroid and write triples belong to it.

        val stats = finalClusters.map(x => (x._2, 1))//x._2: centroid
                                    .reduceByKey(_+_)
                                    .collect//triples in each centroid

        val sw = new PrintWriter(new File(dataset + Constants.statisticsFile + "_" + numOfPartitions))

        stats.foreach{case(partition, size) => {
            if(partition.toString.size == 1)
                sw.write("0"+partition.toString + " " + size + "\n")
            else
                sw.write(partition.toString + " " + size + "\n")
        }}
        sw.close
    }
    
    //create folders from the files in the previous step.
    def subPartitionClusters(spark: SparkSession, dataset: String, hdfs: String, partitionNum: Int) = {
        import spark.implicits._

      import org.apache.spark.sql.functions._
      //import sqlContext.implicits._

        val sc = spark.sparkContext

        val basePath = hdfs + dataset + Constants.clusters + "_" + numOfPartitions + "_bal"
        val utilPath = dataset + Constants.clusters + "_" + numOfPartitions + "_bal"
        val partitions = HdfsUtils.getDirFilesNames(hdfs + utilPath)

        //val nodeIndex = Loader.loadNodeIndex(spark, partitionNum, dataset, hdfs)
        //nodeIndex.cache() //maybe


      //-------------------GENERATE NODE INDEX----------------------------------
        val fullPath = hdfs + dataset + Constants.clusters + "_"+ numOfPartitions + "_bal/" //files for partitions created by partitionClusters()
        val cluster = Loader.loadClusters(spark, fullPath).distinct //load all partitioned files? .parquet?

        val nodeIndex = cluster.rdd.filter{case(s, o, p) => p == Constants.RDF_TYPE}
          .map{case(s, o, p) => {
            (s, o) //(uri, typeOfURI)
          }}

        val nodeIndexDf = nodeIndex.map(node => Types.NodeIndex(node._1, node._2)).toDF() //(uri, typeOfURI)
        println("generateNodeIndex ------------ "+ hdfs + dataset + Constants.nodeIndexFile + "_" + numOfPartitions)
        nodeIndexDf.write.format("parquet").save(hdfs + dataset + Constants.nodeIndexFile + "_" + numOfPartitions)

       // val nodeIndexDT = nodeIndexDf .withColumnRenamed("_1", "s")
                     //                  .withColumnRenamed("_2", "c")
                    //                  .as[(String, String)]
      //---------------------------------------------------------------------

      partitions.foreach(p => {
        val fullPath = basePath + "/" + p // p: part-00000-4dd825ad-46db-43a8-9ea9-09092757d2aa-c000.snappy.parquet
        val partition = Loader.loadClustersWithId(spark, fullPath) //dataFrame - .parquet files
          .withColumnRenamed("_1", "s")
          .withColumnRenamed("_2", "o")
          .withColumnRenamed("_3", "p")
          .as[(String, String, String)]

        val start = p.indexOf("part-") + 5
        val partitionName = p.substring(start, start+5) //take 00000, 00001 ...
        println("1.partitionName:"+partitionName)
        val partSize = partition.rdd.partitions.size
        println(partSize)
        println("roundUp(partSize/7.0): "+roundUp(partSize/7.0))
        partition.rdd.repartition(roundUp(partSize/7.0)) //partition.rdd.repartition(roundUp(partSize/7.0))
          .filter{case(s, o, p) => p.size < 150}
          .toDF.as[(String, String, String)]
          .map{case(s, o, p) => (s, o, uriToStorage(p))}
          .write.partitionBy("_3").parquet(basePath + "/" + partitionName) //create folders for each predicate inside to each folder of cluster

        /*partition.repartition(col("p"))
          .filter(_._3.size <150)
          .map{case(s, o, p) => (s, o, uriToStorage(p))}
          .write.partitionBy("_3").parquet(basePath + "/" + partitionName) //create folders for each predicate inside to each folder of cluster

      })*/
         //troulin: remove file
          //HdfsUtils.removeFilesInHDFS(fullPath)
      })

      //######################################## VP INDEX -  UNBOUND PREDICATES IN QUERIES ##################################################
      var classSubPartIndex = Seq.empty[Types.NodeSubPartIndex].toDS()
      partitions.foreach(p => { //p: part-00000-4dd825ad-46db-43a8-9ea9-09092757d2aa-c000.snappy.parquet  -- SWDF_data/cluster/clusters_8_bal/00000
        val fullPath = basePath + "/" + p // p: part-00000-4dd825ad-46db-43a8-9ea9-09092757d2aa-c000.snappy.parquet
        val partition = Loader.loadClustersWithId(spark, fullPath) //dataFrame - .parquet files
          .withColumnRenamed("_1", "s")
          .withColumnRenamed("_2", "o")
          .withColumnRenamed("_3", "p")
          .as[(String, String, String)]

        val start = p.indexOf("part-") + 5
        val partitionName = p.substring(start, start + 5) //take 00000, 00001 ...
        //val partSize = partition.rdd.partitions.size
        //println(partSize)

        //todo: keep also uris without typed declaration ?? maybe yes! Remove them (filter -exist- in nodeIndex)?
       /* val sub = partition.rdd.map{case(s,o,p) => (s, List(uriToStorage(p)))}.reduceByKey(_:::_) //:::  -> concat elements of lists in one list
                      //  .map{case(inst, parts) => (inst, parts.mkString(",")) }    //(inst, List(subParts))
                       // .map(node => Types.NodeSubPartIndex(node._1 + "__" + partitionName, node._2)).toDF()

        val obj = partition.rdd.map{case(s,o,p) => (o, List(uriToStorage(p)))}.reduceByKey(_:::_) //:::  -> concat elements of lists in one list
          //.map{case(inst, parts) => (inst, parts.mkString(",")) }    //(inst, List(subParts))
         // .map(node => Types.NodeSubPartIndex(node._1 + "__" + partitionName, node._2)).toDF()

         val sub_obj = sub.union(obj).reduceByKey(_:::_) //:::  -> concat elements of lists in one list
                      .map{case(inst, parts) => (inst, parts.mkString(",")) }    //(inst, List(subParts))
                      .map(node => Types.NodeSubPartIndex(node._1 + "__" + partitionName, node._2)).toDF()
      */

        //-----Keep only typed declared uris----
        val sub = partition.join(nodeIndexDf, partition("s") === nodeIndexDf("uri"))
          // .select(nodeIndexDf("uriType"), partition("p"))
          .map(row => ("s" + uriToStorage(row.getString(2)), row.getString(0)) ) //(partition, uri) --Dataset[String, String]

        val obj = partition.join(nodeIndexDf, partition("o") === nodeIndexDf("uri"))
          // .select(nodeIndexDf("uriType"), partition("p"))
          .map(row => ("o" +  uriToStorage(row.getString(2)), row.getString(1)) ) //(partition, uri) --Dataset[String, String]

        //todo: more predicates are connected with claesses
        val objClass  = partition.where(col("p") === Constants.RDF_TYPE).select(partition("o")).distinct()
                                  .map(r => ("o" +  uriToStorage(Constants.RDF_TYPE), r.getString(0)))
        //val objClass = partition.filter(tp => tp._3 == Constants.RDF_TYPE).distinct()//.map(tpC => tpC._3).distinct()
       // println("objClass:"+objClass.count())
        val totalObj = obj.union(objClass)

        val urisUNION = sub.union(totalObj)

        val uris = urisUNION.groupBy("_2") //uri
          .agg(collect_set("_1") as "subPart").map(x => Types.NodeSubPartIndex (x.getString(0) , x.getSeq(1).mkString(","))) //(uri, subParts)

        uris.write.format("parquet").save(hdfs + dataset + Constants.classSubPartIndexFile + "_" + numOfPartitions+ "__" + partitionName)

      })

    //############################################################################################################################

      //val subPart = Loader.loadSubPartIndex(spark, partitionNum, dataset, hdfs, "00000")
      //<http://data.semanticweb.org/conference/dh/2010/abstracts/paper/ab-795>__00000
      //subPart.limit(3).take(1).foreach(x => println(x.uri + " -- "+x.parts))

    }


    def uriToStorage(uri: String) = {
        uri.replaceAll("[^A-Za-z0-9]", "_").toLowerCase
    }


    def fillStr(str: String) = {List.fill(5-str.length)("0").mkString + str}
    
    /**
    * Builds an index of node to class
    */
      //uri, typeOfURI -- create file for uris and the corresponding classes
    def generateNodeIndex(spark: SparkSession, dataset: String, hdfs: String) = {
        import spark.implicits._
        val fullPath = hdfs + dataset + Constants.clusters + "_"+ numOfPartitions + "_bal/" //files for partitions created by partitionClusters()
        val cluster = Loader.loadClusters(spark, fullPath).distinct //load all partitioned files? .parquet?
        val nodeIndex = cluster.rdd.filter{case(s, o, p) => p == Constants.RDF_TYPE}
                                    .map{case(s, o, p) => {
                                        (s, o) //(uri, typeOfURI)
                                    }}
        
        val nodeIndexDf = nodeIndex.map(node => Types.NodeIndex(node._1, node._2)).toDF() //(uri, typeOfURI)
        //println("generateNodeIndex ------------ "+ hdfs + dataset + Constants.nodeIndexFile + "_" + numOfPartitions)
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

    
    //locate triples in centroids
    def partitionInstances(sc: SparkContext, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String], dataset: String, hdfs: String): RDD[((String, String, String), String)] = {
        val classIndex = Loader.loadClassIndexBal(numOfPartitions, dataset) //Map[String, Array[String]]  //(nodeUri -> partitions) -- (node \t id_centroid1, id_centroid2,..)
        // val centroidMap = Loader.loadCentroiMap(numOfPartitions, dataset)//.map(x => (x._2, x._1))
        //val schemaCluster = Loader.loadSchemaCluster(sc, numOfPartitions).map{case(t, cId) => (t, centroidMap(cId).toString)}

        val brIndex = sc.broadcast(classIndex) //broadcast MAP -- (nodeUri -> partitions_id())
        // val brMap = sc.broadcast(centroidMap)
        // Types.InstanceVertex(id, (uri, types, labels))
        val partitionedInstances = instanceGraph.triplets.filter(t => !t.srcAttr._2.isEmpty || !t.dstAttr._2.isEmpty)
                                                .map(t => ((t.srcAttr._1, t.dstAttr._1, t.attr), (t.srcAttr._2 ++ t.dstAttr._2).toArray.distinct)) //t.srcAttr._1: id , t.srcAttr._2: URI /class?, t.srcAttr._3: case(p, o) -label
                                                .map{case(t, types) => (t, types.map(brIndex.value.getOrElse(_, Array())))} //(t, (Array(Array(id_centroid()), Array(id_centroid())) -- t:  (t.srcAttr._1, t.dstAttr._1, t.attr) of triple
                                                .map{case(t, partitions) => (partitions.reduce((a, b) => (a ++ b)).distinct.map(p => (t, p)))} //rdd(Array) -- Array[(t:(src, dst, attr), id_centroid1), ...] --same triple to different centroids!
                                                .flatMap(t => t) //rdd[tuples]: ((src1, dst1, attr1), id_centroid1),   ((src2, dst2, attr2), id_centroid2), ...
                                                .distinct

        //Create and partition RDF_TYPE triples
        val typeTriples = instanceGraph.triplets.filter(t => !t.srcAttr._2.isEmpty || !t.dstAttr._2.isEmpty)
                                        .map(t => t.srcAttr._2.map(typeUri => (t.srcAttr._1, Constants.RDF_TYPE, typeUri)) // (src, RDF:TYPE, Class_URI) -- t.srcAttr._2: URI /class?
                                                ++ t.dstAttr._2.map(typeUri => (t.dstAttr._1, Constants.RDF_TYPE, typeUri)))// ++  (dst, RDF:TYPE, Class_URI) -- t.srcAttr._2: URI /class?
                                        .flatMap(t => t).distinct

        val partitionTypeTriples = typeTriples.map{case(t) => (t, brIndex.value.getOrElse(t._3, Array()))} // [(src, RDF:TYPE, Class_URI), (id_centroid() ) ]
                                                .filter(!_._2.isEmpty)
                                                .map{case(t, partitions) => partitions.map(p => (t, p))} //[(src, RDF:TYPE, Class_URI), id_centroid() )]
                                                .flatMap(t => t)
                                                .map{case(t, p) => ((t._1, t._3, t._2), p)} //(src, Class_URI, RDF:TYPE), (Class_URI,id_centroid() )]
                                                .distinct
    
        val labelTriples = instanceGraph.triplets.filter(t => (!t.srcAttr._3.isEmpty && !t.srcAttr._2.isEmpty) || (!t.dstAttr._3.isEmpty && !t.dstAttr._2.isEmpty)) //t.srcAttr._3: case(p, o)
                                        .map(t => populateLabels(t.srcAttr) ++ populateLabels(t.dstAttr)) //[(uri, o, p), types] -- types of src or dst
                                        .flatMap(t => t)
                                        .map{case(t, types) => (t, types.map(brIndex.value.getOrElse(_, Array())))} //(uri, o, p), (id_centroid())]
                                        .filter(!_._2.isEmpty)
                                        .map{case(t, partitions) => (partitions.reduce((a, b) => (a ++ b)).distinct.map(p => (t, p)))}
                                        .flatMap(t => t)
                                        .distinct
        //partitionedInstances: [(src_uri, (IDcentr1, IDcentr2,..)] -- src_uri: all instances
        val finalClusters = /*schemaCluster.union(*/partitionTypeTriples.union(partitionedInstances).union(labelTriples).distinct //[t:(src, dst, attr), ( id_centroid,..) ] in all subpartitions

      //instanceGraph.triplets.take(2).foreach(k => println("take2. "+ k.srcAttr._1 + " - " +  k.dstAttr._1 + " - " + k.attr))

      //instanceGraph.triplets.filter(t => t.srcAttr._1 == "<http://data.semanticweb.org/conference/eswc/2006/talks/paper-presentation-ontology-evaluation-ma>")
       //             .collect().foreach(k => println("0. "+ k.srcAttr._1 + " - " +  k.dstAttr._1 + " - " + k.attr))

      // partitionedInstances.collect().filter{case(((src, dst, attr), id_centroid)) => src == "<http://data.semanticweb.org/conference/eswc/2006/talks/paper-presentation-ontology-evaluation-ma>"}
        //.foreach{case(((src, dst, attr), id_centroid)) => println("2. "+id_centroid+": "+  src +" -- "+dst +" -- "+ attr)}

      //partitionTypeTriples.filter{case(((src, dst, attr), id_centroid)) => src == "<http://data.semanticweb.org/conference/eswc/2006/talks/paper-presentation-ontology-evaluation-ma>"}
        //.collect().foreach{case(((src, dst, attr), id_centroid)) => println("1. "+id_centroid+": "+  src +" -- "+dst +" -- "+ attr)}

      //finalClusters.filter{case(((src, dst, attr), id_centroid)) => src == "<http://data.semanticweb.org/conference/eswc/2006/talks/paper-presentation-ontology-evaluation-ma>"}
      //  .collect().foreach{case(((src, dst, attr), id_centroid)) => println("3. "+id_centroid+": "+  src +" -- "+dst +" -- "+ attr)}

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
    def rdfCluster(spark: SparkSession,  //edge: (s, o, p) vertices: (id, (uri, types, labels))
                    instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String],  //(String, Seq[String]: (e.id, e.data) Edge(e.src, e.dst, e.e)
                    schemaGraph: Graph[String, (String, Double)],  //edge: Edge(e.src, e.dst, e.e) -- vertices: (e.id, e.uri)
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
    */ // map(node =>(node._1, dependence(importanceMap, node._2, maxDiff, minDiff)))
    def dependence(importanceMap: Map[Long, Double],
                    centroidMap: Map[Long, Tuple2[Double, Seq[Tuple3[Long, Long, String]]]], 
                    max: Double, min: Double) = {
        centroidMap.map(x => (x._1, (calculateDependence(importanceMap(x._1), x._2, max, min), x._2._2))) //x._2._2: Tuple3[Long, Long, String]?
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
        //importance.foreach(x => println("Imp. "+x._1+ " :  "+x._2))
        importance.filter(x => schemaMap(x._1).contains("http")).toSeq.sortWith(_._2 > _._2).map{case(x,y) => x}.take(k)
    }
}