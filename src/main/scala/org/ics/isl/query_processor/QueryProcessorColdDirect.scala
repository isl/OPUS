package org.ics.isl

import scala.io.Source
import java.io._
import sys.process._
import scala.concurrent.duration._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import scala.collection.Map
import scala.collection.immutable.ListMap

/**
 * Query Processor Direct using SQL
 *
 */
object QueryProcessorColdDirect {
    val spark = loadSparkSession()
    
    var dataMap = Map[String, Dataset[Row]]()
    var partitionNum = -1
    var subPartitionType = ""
    var subPartitionMode = ""
    var bal = -1



    def main(args: Array[String]): Unit = {
         import spark.implicits._
        
        if(args.size != 6) {
            println("Arguments: lood_id, number of partitions, sub partitioning mode, query file ,dataset name, balance")            
            System.exit(-1)
        }
        val queryFile = new File(args(3))
        val dataset = args(4)
        val loopId = args(0)
        partitionNum = args(1).toInt
        subPartitionMode = args(2)
        bal = args(5).toInt
        
        val sc = spark.sparkContext

        var inputPath = "/home/jagathan/test_queries/" + dataset + "/indexed/translated_queries_2_" + subPartitionMode + "_" + partitionNum + "/"
        var resultPath = if(bal == 1){
            "../results/" + dataset + "/cold_" + partitionNum + "_" + subPartitionMode + "_bal/indexed/"
        }
        else {
            "../results/" + dataset + "/cold_" + partitionNum + "_" + subPartitionMode + "/indexed/"
        }
        
    
        val queryPath = queryFile.getPath
        val queryName = queryFile.getName
        
        HdfsUtils.removeDirInHDFS(dataset + "/result_cold_" + subPartitionMode +"/")
        println(queryPath)          

        //Parse query
        val file = Source.fromFile(queryFile).getLines

        var partitions = Array[String]()
        var tpNum: Int = -1
        var queryMap = Array[(String, String)] ()
        for(line <- file) {
            if(line.startsWith("TP")){
                tpNum = line.split(" ")(1).toInt
            }
            else if(line.startsWith("partitions")){
                partitions = line.split(" ")(1).split(",")
            }
            else if(!line.startsWith(">>>>>")) {
                val tokens = line.split(" ")
                val key = tokens(0)//.split("_")(1) uncomment here and in func call for version 2
                val query = tokens.drop(1).mkString(" ")
                queryMap = queryMap :+ (key, query)
            }
        }

        val resultFile = new File(resultPath + "results" + "_" + tpNum + "_" + loopId + ".txt")
        val resultWriter = new FileWriter(resultFile, true) //appends
              
        val (executionTime, result) = executeNonTypeQuery(queryMap.map(_._2)/*.sortBy(_._1).map(_._2)*/, partitions, dataset, queryName)

        resultWriter.append("Query: " + queryFile.getName + "\n")
        resultWriter.append("Time: " + executionTime + "\n")
        resultWriter.append("Result_count: " + result.count + "\n")
        resultWriter.append("partitions: " + partitions.mkString(",") + "\n")
        
        resultWriter.close
        spark.stop()
    }

  
    /**
    * Initializes spark session
    */
    def loadSparkSession() = {
        val spark = SparkSession.builder
                                .appName("QueryProcessor")
                                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                .config("spark.speculation", "true")
                                .config("spark.sql.autoBroadcastJoinThreshold", "60817408") //200M 209715200 100M 104857600 50M 52428800
                                .config("spark.sql.inMemoryColumnarStorage.batchSize", 100000)
                                .config("spark.sql.inMemoryColumnarStorage.compressed", true)
                                .config("spark.sql.crossJoin.enabled", "true")
                                .getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext  
        sc.setLogLevel("ERROR") 
        spark
    }

    /**
    * handles queries that do not have rdf:type
    */
    def executeNonTypeQuery(queryMap: Array[String], partitions: Array[String], dataset: String, qName: String): (Long, Dataset[Row]) = {
        preloadTables(partitions)
        val result = executeFinalQuery(queryMap)
        var t1 = System.nanoTime()
        result.write.csv(Constants.HDFS + dataset + "/result_cold_" + subPartitionMode +"/")
        var t2 = System.nanoTime()
        var duration = (t2 - t1) / 1000 / 1000
        (duration, result)
    }

    def executeFinalQuery(queryMap: Array[String]): Dataset[Row] = {
        if(queryMap.size == 1){
            val query = queryMap(0)
            spark.sql(query)
        }
        else {
            queryMap.map(query => spark.sql(query)).reduceLeft((left, right) => (customJoin(left, right)))    
        }
    }

    def customJoin(left: Dataset[Row], right: Dataset[Row]): Dataset[Row] = {
        val commonCols = commonColumns(left, right)

        if(commonCols.size > 0){
            val result = left.join(right, commonCols, "inner")
            result
        }
        else {
            val result = left.crossJoin(right)
            result
        }
    }

    def preloadTables(partitions: Array[String]) = {
        partitions.foreach{case(partition) => {
            val dataset = loadDataset(partition)
            val cleanName = "table" + partition.replace("-", "_").replace("=", "_E_").trim
            dataset.createOrReplaceTempView(cleanName)
        }}
    }
    
    /**
    * returns true if an array contains only empty Strings
    */
    def isEmptyArray(array: Array[String]): Boolean = {
        array.foreach(x => if(!x.isEmpty) return false)
        return true
    }

    /**
    * tranforms map to mutable
    */
    def toMutable[A, T](map: scala.collection.immutable.Map[A, T]) = {scala.collection.mutable.Map() ++ map}
    
    /**
    * project result on variables
    */
    def projectResult(result: Dataset[Row], variables: Array[String]) = {
        if(isEmptyArray(variables))
            result
        else
            result.select(variables.head, variables.tail: _*)
    }    

    def commonColumns(left: Dataset[Row], right: Dataset[Row]) = {
        left.columns.intersect(right.columns).toSeq
    }

    /**
    * replaces prefix in triple patterns
    */
    def replacePrefix(prefixMap: Map[String, String], triplePatternsStr: String) = {
        var tps = triplePatternsStr
        prefixMap.foreach{case(k, v) => {
            tps = tps.replaceAll(k + ":", v)
        }}
        tps
    }
    
    /**
    * Calculates the  number of variables in a triple pattern
    */
    def numberOfVars(triplePattern: (String, String, String)) = {
        triplePattern.productIterator.filter(_.toString.contains("?")).size
    }

    /**
    * Finds variables in a triple patterm
    */
    def findTPVars(tp: Tuple3[String, String, String]): Array[String] = {
        tp.productIterator.zipWithIndex
                            .filter(_._1.toString.contains("?"))
                            .map(x =>  QueryParser.removeNonAlphaNum(x._1.toString))
                            .toArray
    }

    def loadDataset(file: String): Dataset[Row] = {
        import spark.implicits._ 
        //load file pointed by index
        var input: String = ""
        // println(file)
        if(file.contains("-")) {
            val tokens = file.split("-")
            val partition = if(tokens(0).endsWith("_s_") ){
                tokens(0).replace("_s_", "/s/")
            } else if (tokens(0).endsWith("_o_")) {
                tokens(0).replace("_o_", "/o/")
            } else {
                tokens(0)
            }
            val subPartition = tokens(1)
            input = if(bal == -1) {
                Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + partitionNum + "/" + partition + "/" + subPartition + "/*"
            }else {
                Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + partitionNum + "_bal/" + partition + "/" + subPartition + "/*"                
            }
        }
        else if(subPartitionMode == "baseline") {
            input = Constants.HDFS + Constants.baseLine
        }
        else {
            input = if(bal == -1) {
                Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + partitionNum + "/" + file + "/*"
            }else {
                Constants.HDFS + Constants.clusters + "_" + subPartitionMode + "_" + partitionNum + "_bal/" + file + "/*"
            }
        }
        
        if(!dataMap.contains(file) && (subPartitionMode == "vp" || subPartitionMode == "baseline")){
            val dataset = spark.read.load(input)
                                    .as[(String, String/*, String*/)]
                                    .withColumnRenamed("_1", "s")
                                    .withColumnRenamed("_2", "o")
                                   // .withColumnRenamed("_2", "p")
            dataMap = dataMap + (file -> dataset)
        }
        else if (!dataMap.contains(file)) {
            val dataset = spark.read.load(input)
                                    .as[(String, String, String)]
                                    .withColumnRenamed("_1", "s")
                                    .withColumnRenamed("_2", "o")
                                    .withColumnRenamed("_2", "p")
            dataMap = dataMap + (file -> dataset)   
        }
        return dataMap(file)
    } 

    /**
    * Returns list of files of the given folder
    */
    def getListOfFiles(dir: String):List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList
        } else {
            List[File]()
        }
    }

    def listFiles(base: File, recursive: Boolean = true): Seq[File] = {
        val files = base.listFiles
        val result = files.filter(_.isFile)
        result ++
          files
            .filter(_.isDirectory)
            .filter(_ => recursive)
            .flatMap(listFiles(_, recursive))
    }

   
}