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
 * Query Processor using SQL
 *
 */
object QueryTranslatorVP2 {
    val spark = loadSparkSession()   
    var partitionNum = -1
    var subPartitionType = ""
    var subPartitionMode = ""
    var balance = -1
    def main(args: Array[String]): Unit = {
        import spark.implicits._
        val sc = spark.sparkContext
        val options = Array("indexed"/*, "no_indexed"*/)
        if(args.size < 4){
            println("Missing Arguments")
            println("Arguments: number of partitions, sub partitioning mode, dataset name, balance")
            System.exit(-1)
        }

        val dataset = args(2)
        partitionNum = args(0).toInt    
        subPartitionMode = args(1)
        balance = args(3).toInt

        var inputPath = "/home/jagathan/test_queries/" + dataset + "/indexed/sparql/"
        
        val nodeIndex = Loader.loadNodeIndex(spark, partitionNum)
        
        val classIndex = if(balance == 1) {
            Loader.loadClassIndexBal(partitionNum, subPartitionMode)
        }
        else {
            Loader.loadClassIndex(partitionNum)
        }
        
        val statistics = if(balance == 1) {
            Loader.loadPartitionStatsBal(partitionNum, subPartitionMode)
        }
        else {
            Loader.loadPartitionStats(partitionNum, subPartitionMode)   
        }

        var j = 0       
        val fileList = listFiles(new File(inputPath), true)
        nodeIndex.cache
                    
        fileList.foreach(queryFile => {

            val queryPath = queryFile.getPath
            val queryName = queryFile.getName
            val slicedQueryPath = queryPath.replace(queryPath.slice(queryPath.lastIndexOf("/"), queryPath.length), "")
            //subdir for number of triple patterns
            val subDir = slicedQueryPath.substring(slicedQueryPath.lastIndexOf("/")+1, slicedQueryPath.length)
            // println(subDir)
           // if(subDir.contains("5")) {
                //folder until /indexed
                println(slicedQueryPath)
                val rootFolder = slicedQueryPath.substring(0, slicedQueryPath.lastIndexOf("sparql"))
                //create translated queries folder
                val translateFolder = if(balance == 1) {
                    rootFolder + "/" + "translated_queries_2" + subPartitionMode + "_" + partitionNum + "_bal"
                }
                else {
                    rootFolder + "/" + "translated_queries_2" + subPartitionMode + "_" + partitionNum
                }
                
                if(!new File(translateFolder).exists) {
                    val mkdir = "mkdir " + translateFolder !
                }
                val fullPath = translateFolder + "/" + subDir + "/"
                if(!new File(fullPath).exists) {
                    val mkdir = "mkdir " + fullPath !
                }
                
                //Parse query
                QueryParser.parseQuery(queryFile)
                
                val variables = QueryParser.variables
                val triplePatterns = QueryParser.triplePatterns
                val queryMappings = QueryParser.queryMappings


                val queryIndex: Map[Int, String] = buildQueryIndex(queryMappings, triplePatterns, nodeIndex, classIndex, statistics)
                println(queryPath)
                if(QueryParser.isIndexedQuery && !queryIndex.contains(-1)) {
                    val pw = new PrintWriter(new File(fullPath + queryName))

                    val aggrTps = aggregateTps(triplePatterns)

                    val queryMap = translateTp(aggrTps, toMutable(triplePatterns.toMap), queryIndex)
                    pw.write(">>>>> " + queryName + "\n")
                    queryMap.foreach{case(k, v) => {
                        pw.write(k + " " + v + "\n")
                    }}
                    val partitions = queryIndex.values.toArray.distinct.mkString(",")
                    pw.write("partitions " + partitions + "\n")
                    pw.write("TP " + triplePatterns.size)
                    pw.close

                }
                else {
                    println("Query cannot be indexed")
                    // val prevFolder = queryPath.substring(0, queryPath.indexOf("indexed"))
                    // println("prevFolder " + prevFolder)
                    // val noIndexPath = prevFolder + "no_indexed" + "/"
                    // if(!new File(noIndexPath).exists){
                    //     val cmd = "mkdir " + noIndexPath !
                    // }
                    // val cmd = "mv " + queryPath + " " + noIndexPath !
                }
          //  }
        })
        j+=1
  
        spark.stop()
    }

     def organizeQueries(triplePatterns: Array[(Int, (String, String, String))], queryFile: File, inputPath: String) = {
        val size = triplePatterns.size
        val cmd = "mv " + inputPath + queryFile.getName + " " + inputPath + size + "/" + queryFile.getName !
    }
    
    def buildQueryIndex(queryMappings: Map[Int, String], triplePatterns: Array[(Int, (String, String, String))],
                             nodeIndex: Dataset[Types.NodeIndex], classIndex: Map[String, Array[String]], 
                            statistics: Map[String, Int]): Map[Int, String] = {
        //Query index map each tp to a partition
        val queryIndex = queryMappings.map{case(tpId, uri) => {
            //check if uri is class uri
            val c = classIndex.getOrElse(uri, Array()).size
            //uri is class uri
            if(c > 0){
                val partition = classIndex(uri).map(p => List.fill(5-p.length)("0").mkString + p).map(p => (p, statistics(p))).minBy(_._2)
                //clean partition name for storage format
                val cleanPartName = List.fill(5-partition._1.length)("0").mkString + partition._1
                (tpId, cleanPartName)
            } 
            else {
                //get uri classes
                val types = nodeIndex.where("uri='" + uri + "'").distinct.collect.map(_.uriType).toArray

                //TODO FIX 
                //if does not intersect we have to add to separate array and find partial mapSubPartitions
                
                //find intersect of all partitions of all its classes
                //if does not intersect add arrays
                
                //if uri does not have class (cannot be indexed)
                if(types.size == 0)
                    (-1, "-1")
                else{
                    //Every node may have more than one types.
                    //Find common intersection of sets OR if set does not intersect unite them.
                    val partition = types.map(t => classIndex.getOrElse(t, Array("-1")))
                                    .reduceLeft((a, b) => if(a.intersect(b).isEmpty) a++b else a.intersect(b))
                    if(partition.contains("-1"))
                        (-1, "-1")
                    else {
                        val minPartName = partition.map(p => List.fill(5-p.length)("0").mkString + p)
                                                .map(t => (t, statistics.getOrElse(t, -1)))
                                                .minBy(_._2)._1
                        //clean partition name for storage format
                        val cleanPartName = List.fill(5 - minPartName.length)("0").mkString + minPartName
                        (tpId, cleanPartName)
                    }   
                }
            }
        }}
        if(queryIndex.contains(-1))
            return queryIndex
        else
            return mapSubPartitions(queryIndex, triplePatterns)
    }

    def mapSubPartitions(queryIndex: Map[Int, String], triplePatterns: Array[(Int, (String, String, String))]) = {
        queryIndex.map{case(tpId, partition) => {
            val tp = triplePatterns.find(_._1 == tpId).get._2
            if(QueryParser.isVariable(tp._2)){
                (tpId, partition)
            }
            else {
                val predicate = "_3=" + uriToStorage(tp._2)
                val subPartition = partition + "-" + predicate
                (tpId, subPartition)    
            }
        }}
    }
    
    def uriToStorage(uri: String) = {
        uri.replaceAll("[^A-Za-z0-9]", "_").toLowerCase
    }



    def containsOnly[A](array: Array[A], element: A): Boolean = {
        var count = 0
        array.foreach(curElem => {
            if(curElem == element)
                count+=1
        })
        if(count == array.size)
            return true
        else
            false
    }

   
    /**
    * Initializes spark session
    */
    def loadSparkSession() = {
        val spark = SparkSession.builder
                                .appName("QueryTranslatorVP2")
                                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                .config("spark.speculation", "true")
                                .config("spark.sql.crossJoin.enabled", "true")
                                .getOrCreate()
        import spark.implicits._
        val sc = spark.sparkContext  
        sc.setLogLevel("ERROR") 
        spark
    }

    def aggregateTps(triplePatterns: Array[(Int, (String, String, String))]): Map[String, Array[Int]] = {
        val aggrTps = triplePatterns//.map{case(id, tp) => (findTPVars(tp)(0), Array(id))}.toMap
                                    .flatMap{case(id, tp) => findTPVars(tp).map(x => (x, id))}
                                    .groupBy(_._1)
                                    .map(x => (x._1, x._2.map(_._2)))
        return toMutable(aggrTps)
    }

    /**
    * Translate aggregated triple patterns into SQL. 
    */
    def translateTp(aggrTps: Map[String, Array[Int]], triplePatterns: Map[Int, (String, String, String)], queryIndex: Map[Int, String]): Map[String, String] = {
        var usedIds = Array[Int]()
        var queryMap = Map[String, String]()
        val sortedTps = ListMap(aggrTps.map(x => (x, x._2.size)).toSeq.sortWith(_._2 > _._2):_*).map(_._1)
        var i = 0
        var selectVariables = Set[String]()
        var joinTables = Map[String, Array[String]]()
        var subQueries = Array[String]()
        var varTableMap = Map[String, String]()
        sortedTps.foreach{case(variable, ids) => {
            //remove for removing query reordering
            val sortedIds = ListMap(ids.map(id => (id, findTPVars(triplePatterns(id)).size)).sortWith(_._2 > _._2):_*)//.map(_._1)

            ids.foreach{case(id) => {
                val tp = triplePatterns(id)
                val partition = "table" + queryIndex(id).replace("-", "_").replace("=", "_E_").trim
                println(queryIndex(id))
                println(partition)
                val tableName = "tab" + id
                val subQuery = translate(partition, tp) + " AS " + tableName
                if(joinTables.contains(variable))
                    joinTables = joinTables + (variable -> (joinTables(variable) ++ Array(tableName)))
                else 
                    joinTables = joinTables + (variable -> Array(tableName))
                
                if(!usedIds.contains(id)) {
                    varTableMap = varTableMap ++ extractVarTableMappings(subQuery, tableName)
                    selectVariables = selectVariables ++ findTPVars(tp)
                    subQueries = subQueries :+ subQuery
                    usedIds = usedIds :+ id
                }
            }}
            i+=1
        }}
        var finalQuery = ""
        var joinCondition = " WHERE "
        //val rootTable = joinTables(0) + "." + variable
        finalQuery+= "SELECT " + selectVariables.map(v => varTableMap(v) + "." + v + " AS " + v)
                                    .mkString(",") + " FROM "
        subQueries/*.reverse*/.foreach(q => {  //remove for removing query reordering
            finalQuery = finalQuery + q + ", "
        })

        finalQuery = finalQuery.patch(finalQuery.lastIndexOf(","), "", 2)           
        
        joinTables.foreach{case(v, tables) => {
            for(i <- 0 to tables.length){
                if(i+1 < tables.length){
                    joinCondition = joinCondition + tables(i) + "." + v + "=" + tables(i+1) + "." + v
                    joinCondition = joinCondition + " AND "
                }
            }
        }}

        joinCondition = joinCondition.patch(joinCondition.lastIndexOf(" AND"), "", 4)
        
        if(joinCondition.size > 7)
            finalQuery+= joinCondition
        
        println(finalQuery)
        return queryMap + ("X" -> finalQuery)
    }

    def extractVarTableMappings(query: String, table: String): Map[String, String] = {
        val vars = query.substring(query.indexOf("SELECT") + 6, query.indexOf("FROM"))
        var varTableMap = Map[String, String]()
        vars.split(",").foreach(v => {
            val cleanV = v.substring(v.indexOf("AS")+2).trim
            varTableMap = varTableMap + (cleanV -> table)
        })
        return varTableMap
    }

    def translate(partition: String, tp: (String, String, String)): String = {
        import spark.implicits._ 
        numberOfVars(tp) match {
            case 1 => singleVarTPSql(partition, tp)
            case 2 => doubleVarTPSql(partition, tp)
            case _ => threeVarTPSql(partition, tp) 
        }
    }

    def singleVarTPSql(partition: String, tp: Tuple3[String, String, String]): String = {
        val (s, p, o) = tp
        var query: String = null
        if(s.contains("?")){
            val v = QueryParser.removeNonAlphaNum(s)
            query = "(SELECT s AS " + v + " FROM " + partition + " WHERE o == '" + o + "')"
        }
        else if(p.contains("?")){
            val v = QueryParser.removeNonAlphaNum(p)
            query = "(SELECT p AS " + v + " FROM " + partition + " WHERE " + "s == '" + s + "' AND o == '" + o + "')"
        }
        else{
            val v = QueryParser.removeNonAlphaNum(o)
            query = "(SELECT o AS " + v + " FROM " + partition + " WHERE s == '" + s + "')"
        }
        return query
    }

    def doubleVarTPSql(partition: String, tp: Tuple3[String, String, String]): String = {
        val (s, p, o) = tp
        var query: String = null

        if(s.contains("?") && p.contains("?")) {
            val v1 = QueryParser.removeNonAlphaNum(s)
            val v2 = QueryParser.removeNonAlphaNum(p)
            query = "(SELECT s AS " + v1 + ", p AS " + v2 + " FROM " + partition + " WHERE " + "o == '" + o + "')"
        }
        else if(p.contains("?") && o.contains("?")) {
            val v1 = QueryParser.removeNonAlphaNum(p)
            val v2 = QueryParser.removeNonAlphaNum(o)
            query = "(SELECT p AS " + v1 + ", o AS " + v2 + " FROM " + partition + " WHERE " + "s == '" + s + "')"
        }
        else {
            val v1 = QueryParser.removeNonAlphaNum(s)
            val v2 = QueryParser.removeNonAlphaNum(o)
            query = "(SELECT s AS " + v1 + ", o AS " + v2 + " FROM " + partition + ")"
        }
        return query
    }

    def threeVarTPSql(partition: String, tp: Tuple3[String, String, String]): String = {
        val (s, p, o) = tp
        val v1 = QueryParser.removeNonAlphaNum(s)
        val v2 = QueryParser.removeNonAlphaNum(p)
        val v3 = QueryParser.removeNonAlphaNum(o)
        val query = "(SELECT s AS " + v1 + ", o AS " + v3 + " FROM " + partition + ")"
        return query
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


    /**
    * Cleans triple patterm
    */
    def cleanTriple(triple: Array[String]) = {
          if(triple(2).startsWith("\""))
              (triple(0).drop(1), triple(1).drop(1), triple.drop(2).mkString("> "))
          else
              (triple(0).drop(1), triple(1).drop(1), triple(2).drop(1))
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

    /*
    * Checks if triple pattern contains uri with a ' character.
    * (Avoid syntax error in query execution)
    */
    def isValidQuery(triplePatterns: Array[(Int, (String, String, String))]): Boolean = {
        triplePatterns.foreach{case(id, tp) => {
            if(tp._1.contains("'") || tp._2.contains("'") || tp._3.contains("'"))
                return false
        }}
        return true
    }
}