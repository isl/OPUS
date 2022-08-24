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
object QueryProcessor {
    val spark = loadSparkSession()
    var dataMap = Map[String, Dataset[Row]]()
    var partitionNum = -1
    var subPartitionType = ""
    var firstRun = 0
    var dataset = ""
    var hdfs = ""
    def main(args: Array[String]): Unit = {
        import spark.implicits._

        if(args.size != 4) {
          println("-->Missing Arguments")
          println("-->Arguments: number of partitions, dataset name, hdfs base path, sql query path")
          System.exit(-1)
        }

        dataset = args(0)
        partitionNum = args(1).toInt
        hdfs = args(2)

        if(!hdfs.endsWith("/"))
            hdfs = hdfs + "/"

        //torulin commments: val sc = spark.sparkContext
        val cleanHdfsFolder = HdfsUtils.removeDirInHDFS(hdfs + dataset + "/result"  +"/")
        var inputPath = args(3)

        var resultPath = "results/" + dataset + "/" + partitionNum + "_bal/"

        if(!new File(resultPath).exists){
            val mkdirBase = "mkdir results" !
            val mkdirDataset = "mkdir results/" + dataset !
            val cmd = "mkdir " + resultPath !
        }
        //!!println("inputPath:"+inputPath)
        val fileList = listFiles(new File(inputPath), true)

        fileList.foreach(queryFile => {
            val queryPath = queryFile.getPath
            val queryName = queryFile.getName

            //println("queryPath: "+queryPath)
            //println("queryName: "+queryName)

            //Parse query
            val file = Source.fromFile(queryFile).getLines

            var partitions = Array[String]()
            var tpNum: Int = -1
            var queryMap = Map[String, String] ()
            for(line <- file) {
                if(line.startsWith("TP")){
                    tpNum = line.split(" ")(1).toInt
                }
                else if(line.startsWith("partitions")){
                    partitions = line.split(" ")(1).split(",")
                }
                else if(!line.startsWith(">>>>>")) {
                    val tokens = line.split(" ")
                    val key = tokens(0)
                    val query = tokens.drop(1).mkString(" ")
                    //println("-key: "+key)  //X
                    //println("query: "+query) //query
                    queryMap = queryMap + (key -> query)
                }
            }

            val resultFile = new File(resultPath + "results" + "_" + tpNum + "_" + "bal" + ".txt")

            val resultWriter = new FileWriter(resultFile, true) //appends

            println("Query: " + queryFile.getName)
            val (executionTime, loadTime, loadDataSize, result) = executeNonTypeQuery(queryMap, partitions, dataset, queryName)

            resultWriter.append("Query: " + queryFile.getName + "\n")
            resultWriter.append("Time: " + executionTime + "\n")
            resultWriter.append("Time_load: " + loadTime + "\n")
            resultWriter.append("Load_data: " + loadDataSize + "\n")
            resultWriter.append("Result_count: " + result.count + "\n")
            resultWriter.append("partitions: " + partitions.mkString(",") + "\n")

            println("Time: " + executionTime + "\n")

            resultWriter.close
        })

        spark.stop()
    }


    /**
    * Initializes spark session
    */
    def loadSparkSession() = {

        /*!!val spark = SparkSession.builder.appName("QueryProcessor")
                      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                      .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
                      .config("spark.sql.autoBroadcastJoinThreshold", "-1")
                      .config("spark.sql.parquet.filterPushdown", true)
                      .config("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
                      .config("spark.sql.shuffle.partitions", "50")
                      //.config("spark.sql.adaptive.enabled", true) //spark 3.0
                      //.config("spark.sql.adaptive.skewJoin.enabled", true) //spark 3.0
                      //.config("spark.sql.adaptive.localShuffleReader.enabled", true) //spark 3.0
                      //.config("spark.sql.adaptive.coalescePartitions.enabled", true) //spark 3.0
                      .getOrCreate()
                      */

      val spark = SparkSession.builder.appName("QueryProcessor")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        //.config("spark.executor.memory", "16g")
        .config("spark.sql.inMemoryColumnarStorage.compressed", "true")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.sql.parquet.filterPushdown", true)
        .config("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.cbo.enabled", true) // cost based optimizer
        .config("spark.sql.cbo.joinReorder.enabled", true) // cost based optimizer
        //.config("spark.sql.adaptive.enabled", true) //spark 3.0
        //.config("spark.sql.adaptive.skewJoin.enabled", true) //spark 3.0
        //.config("spark.sql.adaptive.localShuffleReader.enabled", true) //spark 3.0
        //.config("spark.sql.adaptive.coalescePartitions.enabled", true) //spark 3.0
        .getOrCreate()



import spark.implicits._
val sc = spark.sparkContext
sc.setLogLevel("ERROR")
spark
}

/**
* handles queries that do not have rdf:type
*/
def executeNonTypeQuery(queryMap: Map[String, String], partitions: Array[String], dataset: String, qName: String): (Long, Long, Long, Dataset[Row]) = {
  val (loadDataSize, loadTime) =  preloadTables(partitions)
  if(firstRun == 0){
    val result = executeFinalQuery(queryMap)
    //result.foreach(r => println("RES: "+r))
    //var t3 = System.nanoTime()
    result.count
    //var t4 = System.nanoTime()
    //var duration1 = (t4 - t3) / 1000 / 1000
    //println("Duration: "+ duration1)
    firstRun = 1
  }
  val result = executeFinalQuery(queryMap)

  var t1 = System.nanoTime()
  result.count//.write.csv(Constants.HDFS + dataset + "/result_" + subPartitionMode +"/" + qName)
  var t2 = System.nanoTime()
  var duration = (t2 - t1) / 1000 / 1000

  //println(result.explain(true))

  unloadTables(partitions);
  (duration, loadTime, loadDataSize, result)
}

def executeFinalQuery(queryMap: Map[String, String]): Dataset[Row] = {
  if(queryMap.size == 1){
    val query = queryMap.values.toSeq(0)
    spark.sqlContext.sql(query)

    //spark.sqlContext.sql(query)
  }
  else {
    queryMap.map(_._2).map(query => spark.sql(query)).reduceLeft((left, right) => (customJoin(left, right)))
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

def preloadTables(partitions: Array[String]):(Long, Long) = {
  var loadDataSize: Long = 0
  var loadTime: Long = 0
  partitions.foreach{case(partition) => {
    val dataset = loadDataset(partition)

    val cleanName = "table" + partition.replace("-", "_").replace("=", "_E_").trim

    //dataset.createOrReplaceTempView(cleanName)
    //troulin
    //dataset.cache.createOrReplaceTempView(cleanName)

    dataset.createOrReplaceTempView(cleanName)
    spark.sqlContext.cacheTable(cleanName)
    //!!dataset.cache()
    //spark.catalog.cacheTable(cleanName)
    var start = System.currentTimeMillis;
    var size = dataset.count();
    var time = System.currentTimeMillis - start;
    println(cleanName+ ": \t\tCached "+size+" Elements in "+time+"ms");

    loadDataSize = loadDataSize + size
    loadTime = loadTime + time

  }}
  (loadDataSize, loadTime)
}


def unloadTables(partitions: Array[String]): Unit = {
  partitions.foreach { case (partition) => {
    val cleanName = "table" + partition.replace("-", "_").replace("=", "_E_").trim
    var start = System.currentTimeMillis;
    //todo: old method--> spark.sqlContext.dropTempTable(cleanName);
    var ok = spark.catalog.dropTempView(cleanName)
    var time = System.currentTimeMillis - start;
    println(" Uncached  in " + time + "ms ");
    }
  }
  //spark.sqlContext.clearCache()
  spark.catalog.clearCache()
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
    input =  hdfs + dataset + Constants.clusters + "_" + partitionNum + "_bal/" + partition + "/" + subPartition + "/*"
  }else {
    input = hdfs + dataset + Constants.clusters + "_" + partitionNum + "_bal/" + file + "/*"
  }

  if(!dataMap.contains(file)){
    //println("input:"+input)
    val dataset = spark.read.load(input)
                  .as[(String, String/*, String*/)]
                  .withColumnRenamed("_1", "s")
                  .withColumnRenamed("_2", "o")
                  .repartition(30);


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