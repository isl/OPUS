package org.ics.isl

import org.apache.spark.graphx._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.rdd.RDD
import scala.collection.Map
import scala.io.Source
import java.io._

object Loader {
	def loadSchemaGraph(spark: SparkSession, dataset: String, hdfs: String): Graph[String, (String, Double)] = {
		import spark.implicits._
		val edges = spark.read.load(hdfs + dataset + Constants.schemaEdgesFile).as[Types.SchemaEdge].rdd.map(e => Edge(e.src, e.dst, e.e))
		val vertices = spark.read.load(hdfs + dataset + Constants.schemaVerticesFile).as[Types.SchemaVertex].rdd.map(e => (e.id, e.uri))
		Graph(vertices, edges)
	}

	def loadInstanceGraph(spark: SparkSession, dataset: String, hdfs: String): Graph[(String, Seq[String], Seq[(String, String)]), String] = {
		import spark.implicits._
		val edges = spark.read.load(hdfs + dataset + Constants.instanceEdgesFile).as[Types.InstanceEdge].rdd.map(e => Edge(e.src, e.dst, e.e))
		val vertices = spark.read.load(hdfs + dataset + Constants.instanceVerticesFile).as[Types.InstanceVertex].rdd.map(e => (e.id, e.data))
		val ePartNum = edges.partitions.size
		val vPartNum = vertices.partitions.size

		Graph(vertices.repartition(vPartNum*3), edges.repartition(ePartNum*3))
	}

	def createTuple(array: Array[String]):(String, Seq[String]) = {
		val str = array.mkString(",")
		val index = str.lastIndexOf("List(")
		val types: Seq[String] = str.substring(index)
									.drop(5)
									.dropRight(2)
									.split(",")
									.map(x => x.replaceAll(" ", ""))
									.toList
		val vertex = str.substring(0, index-1)
		if(types(0) == "")
			(vertex, Seq[String]())	
		else
			(vertex, types)
	} 


	def loadShortestPaths(sc: SparkContext, k: Int, dataset: String, hdfs: String) = {
		KryoFile.objectFile[Types.ShortestPath](sc, hdfs + dataset + Constants.shortestPaths + "_" + k).map(x => (x.id, x.map))
	}

	def loadCentroiMap(partitionNum: Int, dataset: String): Map[Long, Int] = {
		var centroidMap = Map[Long, Int]()
		for (line <- Source.fromFile(dataset + Constants.centroidMapFile + "_" + partitionNum).getLines) {
			val tokens: Array[String] = line.split(",")
			centroidMap = centroidMap + (tokens(0).toLong -> tokens(1).toInt)
		}
		return centroidMap
	}

	def loadClassIndex(partitionNum: Int, dataset: String): Map[String, Array[String]] = {
		var classIndex = Map[String, Array[String]]()
		for (line <- Source.fromFile(dataset + Constants.classIndexFile + "_" + partitionNum).getLines) {
    		val tokens: Array[String] = line.split("\t")
    		val nodeUri: String = tokens(0)
    		val partitions: Array[String] = tokens.drop(1).mkString.split(",")
    		classIndex = classIndex + (nodeUri -> partitions)
		}
		classIndex
	}

	def loadClassIndexBal(partitionNum: Int, dataset: String): Map[String, Array[String]] = {
		var classIndex = Map[String, Array[String]]()
		for (line <- Source.fromFile(dataset + Constants.classIndexFile + "_" + partitionNum).getLines) {
    		val tokens: Array[String] = line.split("\t")
    		val nodeUri: String = tokens(0)
    		val partitions: Array[String] = tokens.drop(1).mkString.split(",")
    		classIndex = classIndex + (nodeUri -> partitions)
		}
		classIndex
	}

	def loadBaselineClassIndex(partitionNum: Int, versionId: Int, dataset: String): Map[String, Array[String]] = {
		var classIndex = Map[String, Array[String]]()
		for (line <- Source.fromFile(dataset + Constants.classIndexFile + "_" + partitionNum + "_" + versionId.toString).getLines) {
    		val tokens: Array[String] = line.split("\t")
    		val nodeUri: String = tokens(0)
    		val partitions: Array[String] = tokens.drop(1).mkString.split(",")
    		classIndex = classIndex + (nodeUri -> partitions)
		}
		classIndex
	}

	def loadFinalClassIndex(partitionNum: Int, dataset: String): Map[String, Array[String]] = {
		var classIndex = Map[String, Array[String]]()
		for (line <- Source.fromFile(dataset + Constants.finalClassIndexFile + "_" + partitionNum).getLines) {
    		val tokens: Array[String] = line.split("\t")
    		val nodeUri: String = tokens(0)
    		val partitions: Array[String] = tokens.drop(1).mkString.split(",")
    		classIndex = classIndex + (nodeUri -> partitions)
		}
		classIndex
	}

	def subjIndex(subj: String, partitionNum: Int, partition: String, dataset: String): String = {
		// var subjIndex = Map[String, String]()
		for (line <- Source.fromFile(dataset + Constants.subjIndexRoot + "_" + partitionNum + "/" + partition + "/index.txt").getLines) {
    		val tokens: Array[String] = line.split(" ")
    		val id: String = tokens(0)
    		val uri: String = tokens(1)
    		if(subj < uri){
    			return id
    		}
		}
		return "error"
	}

	def objIndex(obj: String, partitionNum: Int, partition: String, dataset: String): String = {
		for (line <- Source.fromFile(dataset + Constants.objIndexRoot + "_" + partitionNum + "/" + partition + "/index.txt").getLines) {
    		val tokens: Array[String] = line.split(" ")
    		val id: String = tokens(0)
    		val uri: String = tokens(1)
    		if(obj < uri){
    			return id
    		}
		}
		return "error"
	}

	def loadStatistics(partitionNum: Int, dataset: String): Map[String, Int] = {
		var statistics = Map[String, Int]()
		for (line <- Source.fromFile(dataset + Constants.statisticsFile + "_" + partitionNum).getLines) {
    		val tokens: Array[String] = line.split(" ")
    		val partition: String = tokens(0)
    		val partitionSize: Int = tokens(1).toInt
    		statistics = statistics + (partition -> partitionSize)
		}
		statistics
	}

	def loadPartitionStats(partitionNum: Int, dataset: String): Map[String, Int] = {
		var statistics = Map[String, Int]()
		for (line <- Source.fromFile(dataset + Constants.partitionStatFile + "_" + partitionNum ).getLines) {
    		if(line.startsWith("00")){
    			val tokens: Array[String] = line.split(" ")
    			val partition: String = tokens(0)
    			val partitionSize: Int = tokens(1).toInt
    			statistics = statistics + (partition -> partitionSize)
    		}
		}
		statistics
	}

	def loadPartitionStatsBal(partitionNum: Int, dataset: String): Map[String, Int] = {
		var statistics = Map[String, Int]()
		for (line <- Source.fromFile(dataset + Constants.partitionStatFile + "_" + partitionNum + "_bal").getLines) {
    		if(line.startsWith("00")){
    			val tokens: Array[String] = line.split(" ")
    			val partition: String = tokens(0)
    			val partitionSize: Int = tokens(1).toInt
    			statistics = statistics + (partition -> partitionSize)
    		}
		}
		statistics
	}

	def loadBaselinePartitionStats(partitionNum: Int, versionId: Int, dataset: String): Map[String, Int] = {
		var statistics = Map[String, Int]()
		for (line <- Source.fromFile(dataset + Constants.partitionStatFile + "_" + partitionNum + "_" + versionId.toString).getLines) {
    		val tokens: Array[String] = line.split(" ")
    		val partition: String = tokens(0)
    		val partitionSize: Int = tokens(1).toInt
    		statistics = statistics + (partition -> partitionSize)
		}
		statistics
	}


	def loadNodeIndex(spark: SparkSession, partitionNum: Int, dataset: String, hdfs: String) = {
    	import spark.implicits._
        spark.read.load(hdfs + dataset + Constants.nodeIndexFile + "_" + partitionNum).as[Types.NodeIndex]
    }

	def loadWeightedGraph(sc: SparkContext, k: Int, dataset: String, hdfs: String) = {
		val edges = sc.textFile(hdfs + dataset + Constants.weightedEdges + "_" + k)
						.map(x => splitSchemaTriple(x))
						.map(x => Edge(x(0).drop(1).toLong, 
									x(1).toLong, (x(2), 
									x(3).dropRight(2).replaceAll(" ", "").toDouble)))

		val vertices = sc.textFile(hdfs + dataset + Constants.weightedVertices + "_" + k)
							.map(x => x.split(",")).map(x => (x(0).drop(1).toLong, x(1).dropRight(1)))

		Graph(vertices, edges)
	}

	def loadClustersWithId(spark: SparkSession, path: String) = {
		import spark.implicits._
        spark.read.load(path).as[(String, String, String)]
	}

	def loadClusters(spark: SparkSession, path: String) = {
		import spark.implicits._
        spark.read.load(path).as[(String, String, String)]
	}

	def cleanTriple(triple: String) = {
		val tripleArray = triple.split(">")
		if(triple.contains("\""))
			(tripleArray(0).drop(2), tripleArray.drop(1).dropRight(2).mkString(">").drop(2).dropRight(1), tripleArray(tripleArray.size - 2).drop(2))
		else
			(tripleArray(0).drop(2), tripleArray(1).drop(2), tripleArray(2).drop(2))
	}
	
	def splitSchemaTriple(triple: String) = {
		val array = triple.split(",")
		val size = array.size
		if(size > 3) {
			Array(array(0), array(1) ,array.drop(2).dropRight(1).mkString(","), array(size-1))
		}
		else {
			array
		}
	}

	def loadCC(sc: SparkContext, dataset: String, hdfs: String):RDD[(String, String, String, Int, Int)] = {
    	sc.textFile(hdfs + dataset + Constants.schemaCC)
    					.map(x => schemaCCParse(x.split(",")))
    					.map(x => (x(0).drop(2), x(1), x(2).dropRight(1), x(3).toInt, x(4).dropRight(1).toInt))
    }

    def loadBC(dataset: String): Map[String, Double] = {
    	var bcMap = Map[String, Double]()
    	for (line <- Source.fromFile(dataset + Constants.huaBCFile).getLines) {
			val tokens: Array[String] = line.split("\t")
			bcMap = bcMap + (tokens(1) -> tokens(2).toDouble)
		}
		bcMap
    }
    

    def loadBaseLine(spark: SparkSession, dataset: String, hdfs: String) = {
    	import spark.implicits._
        spark.read.load(hdfs + dataset + Constants.baseLine).as[(String, String, String)]
    }

    def loadSchemaImportance(dataset: String): Map[Long, Double] = {
    	var impMap = Map[Long, Double]()
    	for (line <- Source.fromFile(dataset + Constants.schemaImportance).getLines) {
			val tokens: Array[String] = line.split("\t")
			impMap = impMap + (tokens(0).toLong -> tokens(1).toDouble)
		}
		impMap
    }

    def loadSchemaNodeFreq(dataset: String): Map[String, Int] = {
    	var freqMap = Map[String, Int]()
    	for (line <- Source.fromFile(dataset + Constants.schemaNodeFrequency).getLines) {
			val tokens: Array[String] = line.split("\t")
			freqMap = freqMap + (tokens(0) -> tokens(1).toInt)
		}
		freqMap
    }

    def loadSchemaNodeCount(dataset: String): Map[String, Int] = {
    	var freqMap = Map[String, Int]()
    	for (line <- Source.fromFile(dataset + Constants.schemaNodeCount).getLines) {
			val tokens: Array[String] = line.split("\t")
			freqMap = freqMap + (tokens(0) -> tokens(1).toInt)
		}
		freqMap
    }

    def schemaCCParse(triple: Array[String]): Array[String] = {
    	val size = triple.size
    	val temp = triple.dropRight(3)
    	if(size > 5)
    		Array(triple(0), temp.drop(1).mkString(","), triple(size - 3), triple(size - 2), triple(size - 1))
    	else
    		triple
    }

    def loadSchemaCluster(sc: SparkContext, k: Int, dataset: String, hdfs: String) = {
    	val schemaCluster = sc.textFile(hdfs + dataset + Constants.schemaClusterFile /*+ "_lessrepl"*/ + "_" + k)
    	schemaCluster.map(x => schemaClusterParse(x.split(",")))
	  					.map(x => ((x(0).drop(2), x(1), x(2).dropRight(1)), x(x.size-1).dropRight(1).toLong))
    }

    def schemaClusterParse(triple: Array[String]): Array[String] = {
    	val size = triple.size
    	if(size > 4)
    		Array(triple(0), triple(1), triple.drop(2).dropRight(1).mkString(","), triple(size - 1))
    	else
    		triple
    }
}