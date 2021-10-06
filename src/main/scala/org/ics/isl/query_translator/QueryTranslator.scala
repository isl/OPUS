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
object QueryTranslator {
    val spark = loadSparkSession()   
    var partitionNum = -1
    var subPartitionType = ""
    //var balance = -1
    var hdfs: String = ""

    def main(args: Array[String]): Unit = {
        import spark.implicits._
        val sc = spark.sparkContext
        if(args.size < 3){
            println("Missing Arguments")
            println("Arguments: dataset name, hdfs base path, sparql query path")
            System.exit(-1)
        }

        val dataset = args(0)
        hdfs = args(1)

        dataset match {
            case "swdf" => partitionNum = 4
            case "lubm"  => partitionNum = 8
            case "lubm8"  => partitionNum = 12
            case "dbpedia"  => partitionNum = 8
            case _ => partitionNum = 4
        }

        if(!hdfs.endsWith("/"))
            hdfs = hdfs + "/"

        var inputPath = args(2)

        val nodeIndex = Loader.loadNodeIndex(spark, partitionNum, dataset, hdfs)
       println("Count nodeIndex:" + nodeIndex.count())
       var subPartIndex = Map[String, Dataset[Types.NodeSubPartIndex]]()
       for( i <- 0 to partitionNum - 1) {
          //if (i != 4 &&  i != 6 && i != 7 ) {
            if(i <10) {
              subPartIndex = subPartIndex + (("0000" + i.toString) -> Loader.loadSubPartIndex(spark, partitionNum, dataset, hdfs, "0000" + i.toString))
              //println(i + "subPartIndex Count:" + subPartIndex("0000" + i.toString).count())
            } else {
              subPartIndex = subPartIndex + (("0000" + i.toString) -> Loader.loadSubPartIndex(spark, partitionNum, dataset, hdfs, "000" + i.toString))
              //println(i + "subPartIndex Count:" + subPartIndex("000" + i.toString).count())
            }


          //}
        }

        val classIndex = Loader.loadClassIndexBal(partitionNum, dataset)
        val statistics = Loader.loadPartitionStatsBal(partitionNum, dataset)

        var j = 0       
        val fileList = listFiles(new File(inputPath), true)
        nodeIndex.cache //[(uri_instance, class)]
                    
        fileList.foreach(queryFile => {

            val queryPath = queryFile.getPath
            val queryName = queryFile.getName
   
            //create translated queries folder
            val translateFolder = inputPath + "/" + "translated_queries" + "_" + partitionNum + "_bal"
            
            if(!new File(translateFolder).exists) {
                val mkdir = "mkdir " + translateFolder !
            }
            val fullPath = translateFolder + "/"// + subDir + "/"
            if(!new File(fullPath).exists) {
                val mkdir = "mkdir " + fullPath !
            }
            
            //Parse query
            QueryParser.parseQuery(queryFile)
            
            val variables = QueryParser.variables //Array[String]
            val triplePatterns = QueryParser.triplePatterns //Array[(Int, (String, String, String))]
            val queryMappings = QueryParser.queryMappings // Map[Int, String] variable: (id -> uriClass) constant: (id->constant)  id (triple pattern): prefer keeping info for variable if exist


            val queryIndex: Map[Int, Array[String]] = buildQueryIndex(queryMappings, triplePatterns, nodeIndex, classIndex, subPartIndex, statistics) //[tpID, partitionName]
            //println("-- queryPath:"+queryPath)
            if(QueryParser.isIndexedQuery && !queryIndex.contains(-1)) {
                val pw = new PrintWriter(new File(fullPath + queryName))

                val aggrTps = aggregateTps(triplePatterns) // [(x1,(id1,id2,..),  (x2, (id2,id4)),..] -- x:variable, id:triple pattern
                println("queryName: "+queryName)
                val queryMap = translateTp(aggrTps, toMutable(triplePatterns.toMap), queryIndex)
                //println("---> "+ queryMap.size)
                pw.write(">>>>> " + queryName + "\n")
                queryMap.foreach{case(k, v) => {
                    //println("\n"+k+ "  *******  :"+v)
                    pw.write(k + " " + v + "\n")
                }}
                //val partitions = queryIndex.values.toArray.distinct.mkString(",")
                val partitions = queryIndex.values.reduce((x, y) => x ++ y).distinct.mkString(",")
                pw.write("partitions " + partitions + "\n")
                pw.write("TP " + triplePatterns.size)
                pw.close

            }
            else {
                println("Query cannot be indexed:"+queryName)

            }
        })
        j+=1
  
        spark.stop()
    }

     def organizeQueries(triplePatterns: Array[(Int, (String, String, String))], queryFile: File, inputPath: String) = {
        val size = triplePatterns.size
        val cmd = "mv " + inputPath + queryFile.getName + " " + inputPath + size + "/" + queryFile.getName !
    }
    
    def buildQueryIndex(queryMappings: Map[Int, String], triplePatterns: Array[(Int, (String, String, String))],
                             nodeIndex: Dataset[Types.NodeIndex], classIndex: Map[String, Array[String]], subPartIndex:Map[String, Dataset[Types.NodeSubPartIndex]],
                            statistics: Map[String, Int]): Map[Int, Array[String]] = {   //[tp_id, partitionName]
        //Query index map each tp to a partition --
        val queryIndex = queryMappings.map{case(tpId, uri) => { //(id -> uri) -- constant: uri_instance, variable(if exist in query Class): uri_Class (?x rdf:type Class)
            //check if uri is class uri
            val c = classIndex.getOrElse(uri, Array()).size// take the number of the centroids/clusters that uri-class is located in.
            val testClass =  classIndex.getOrElse(uri, Array())

            if(c > 0){ //uri is class uri
               //take the smallest cluster
                val partition = classIndex(uri).map(p => List.fill(5-p.length)("0").mkString + p).map(p => (p, statistics(p))).minBy(_._2)   //-- fill in list "0"+p= 00001  5-p.length times
                //println(uri + "IS CLASS!!!")
                //clean partition name for storage format
                val cleanPartName = List.fill(5-partition._1.length)("0").mkString + partition._1
                (tpId, cleanPartName)
            }else { //case that meet constant in triple patterns
                //get uri classes
                val types = nodeIndex.where("uri='" + uri + "'").distinct.collect.map(_.uriType).toArray //?collect???
                //types.foreach(t => println("TYPES:"+t))

                //TODO FIX 
                //if does not intersect we have to add to separate array and find partial mapSubPartitions
                
                //find intersect of all partitions of all its classes
                //if does not intersect add arrays
                
                //if uri does not have class (cannot be indexed)
                if(types.size == 0) //todo: unkonwn uri -- URIunkownIndex[uri, (subPartition)]
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
                       // println("minPartName:"+minPartName+" - "+cleanPartName)

                        (tpId, cleanPartName)
                    }   
                }
            }
        }}
        if(queryIndex.contains(-1))
            return queryIndex.map{case(tpId, parts) => (tpId, Array(parts))} //queryIndex
        else
            return mapSubPartitions(queryIndex, queryMappings, triplePatterns, classIndex, subPartIndex, nodeIndex) //TODO: add partition if predicate is a varable based on NEW index (s/o, partition) ++ small parititon!
    }


    //about predicate files inside clusters
    def mapSubPartitions(queryIndex: Map[Int, String], queryMappings: Map[Int, String],  triplePatterns: Array[(Int, (String, String, String))],  classIndex: Map[String, Array[String]], subPartIndex:Map[String, Dataset[Types.NodeSubPartIndex]], nodeIndex: Dataset[Types.NodeIndex]) = {
        queryIndex.map{case(tpId, partition) => {
            val tp = triplePatterns.find(_._1 == tpId).get._2
            //println(tpId + ">>" + queryMappings.get(tpId).get)  //ex: 0>><http://swrc.ontoware.org/ontology#InProceedings>

            if(QueryParser.isVariable(tp._2)){ //Predicate is variable
                if(!QueryParser.isVariable(tp._1)){
                  val c = classIndex.getOrElse(tp._1, Array()).size
                  //println(tp._1+ " -- 1.c:"+c)
                  /* val types = if (c > 0)  Array(tp._1)
                                else nodeIndex.where("uri='" + tp._1 + "'").distinct.collect.map(_.uriType).toArray //?collect??
                */
                  val part: Dataset[Types.NodeSubPartIndex] = subPartIndex(partition)

                  val subParts = part.where("uri='" + tp._1 + "'").collect.map(res => res.parts)
                 // subParts.foreach(t => println(" 1. SUB-PARTS :"+t))
                 val subPart: Array[String] = subParts.mkString.split(",").filter(_.take(1) == "s").map(sub => sub.substring(1))
                 // subPart.foreach(t => println(" -- 1. subPart :"+t))

                    val subPartition : Array[String] = subPart.map(sp => partition + "-" + "_3=" + uriToStorage(sp))
                    (tpId, subPartition)
                }else if(!QueryParser.isVariable(tp._3) && !QueryParser.isLiteral(tp._3)) {
                  val c = classIndex.getOrElse(tp._3, Array()).size
                  //println("2.c:"+c)
                    /* val types = if (c > 0)  Array(tp._1)
                               else nodeIndex.where("uri='" + tp._1 + "'").distinct.collect.map(_.uriType).toArray //?collect??
                    */
                  val part: Dataset[Types.NodeSubPartIndex] = subPartIndex(partition)
                  val subParts = part.where("uri='" + tp._3 + "'").collect.map(res => res.parts)

                  //subParts.foreach(t => println(" 2. SUB-PARTS :"+t))
                  val subPart: Array[String] = subParts.mkString.split(",").filter(_.take(1) == "o").map(sub => sub.substring(1))
                  //subPart.foreach(t => println(" -- 2.subPart :"+t))

                  val subPartition : Array[String] = subPart.map(sp => partition + "-" + "_3=" + uriToStorage(sp))
                  (tpId, subPartition)


                }else{
                    (tpId, Array(partition))
                }

                //(tpId, partition) //todo: use the NEW index (s/o, subPartition)?

            }
            else {
              val predicate = "_3=" + uriToStorage(tp._2) //predicate: _3=_http___xmlns_com_foaf_0_1_topic_
              val subPartition = partition + "-" + predicate  //partition: 00000 -- subPartition: /00000/_3=_http___xmlns_com_foaf_0_1_topic_
                 (tpId, Array(subPartition))
                //(tpId, subPartition)
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
                                    .flatMap{case(id, tp) => findTPVars(tp).map(x => (x, id))} //(variables, idTriple)
                                    .groupBy(_._1) //Map(x, List(id1, id2,..))
                                    .map(x => (x._1, x._2.map(_._2)))// [(x1,(id1,id2,..),  (x2, (id2,id4)),..] -- x:variable
        return toMutable(aggrTps)
    }

    /**
    * Translate aggregated triple patterns into SQL. 
    */
    //aggrTps: [x1,Array(tpId1,tpId2)] -- triplePatterns: [tpId1, (s,p,o)] -- queryIndex: [tpId1, partition]
    def  translateTp(aggrTps: Map[String, Array[Int]], triplePatterns: Map[Int, (String, String, String)], queryIndex: Map[Int, Array[String]]): Map[String, String] = {
        var usedIds = Array[Int]()
        var queryMap = Map[String, String]()
        val sortedTps = ListMap(aggrTps.map(x => (x, x._2.size)).toSeq.sortWith(_._2 > _._2):_*).map(_._1) //[(variable, (id1, id2)),..] -- ListMap maintains the order of elemnents --First variables contained in more triple.
        var i = 0
        var selectVariables = Set[String]()
        var joinTables = Map[String, Array[String]]()
        var subQueries = Array[String]()
        var varTableMap = Map[String, String]()
        sortedTps.foreach{case(variable, ids) => { // order based on the number of triples that the variable is participated
            //remove for removing query reordering
            //!!val sortedIds = ListMap(ids.map(id => (id, findTPVars(triplePatterns(id)).size)).sortWith(_._2 > _._2):_*)//.map(_._1)

            ids.foreach{case(id) => { //id: triple pattern
                val tp = triplePatterns(id)//(s,p,o)
                //println(variable+ "  TP: "+tp)
                //val partition = "table" + queryIndex(id).replace("-", "_").replace("=", "_E_").trim // table00000__3_E__http___purl_org_dc_elements_1_1_creator_
                //val subParts: Array[String] =  queryIndex(id)
                //val tp = triplePatterns.find(_._1 == tpId).get._2

                if(!QueryParser.isVariable(tp._2)){
                    //val partition = "table" + queryIndex(id).map(i => i.replace("-", "_").replace("=", "_E_").trim) // table00000__3_E__http___purl_org_dc_elements_1_1_creator_
                    val partition = queryIndex(id).map(i => i.replace("-", "_").replace("=", "_E_").trim) // table00000__3_E__http___purl_org_dc_elements_1_1_creator_

                    val tableName = "tab" + id //tab0: triple pattern "0"
                    val subQuery = translate("table" + partition(0), tp) + " AS " + tableName  //(SELECT ...WHERE partition)AS tab1
                    //println("1--> subQuery: \n")

                    if(joinTables.contains(variable))  //[variable, (tab0, tab2)]
                      joinTables = joinTables + (variable -> (joinTables(variable) ++ Array(tableName)))
                    else
                      joinTables = joinTables + (variable -> Array(tableName))

                    if(!usedIds.contains(id)) {
                      varTableMap = varTableMap ++ extractVarTableMappings(subQuery, tableName)
                      //varTableMap.foreach(v => println("<<  "+v))
                      selectVariables = selectVariables ++ findTPVars(tp)
                      subQueries = subQueries :+ subQuery
                      usedIds = usedIds :+ id
                  }

                }else{ //UNION since predicate of triple is variable
                  //val partition = "table" + queryIndex(id).map(i => i.replace("-", "_").replace("=", "_E_").trim) // table00000__3_E__http___purl_org_dc_elements_1_1_creator_

                  //val partition = queryIndex(id).map(i => i.replace("-", "_").replace("=", "_E_").trim) // table00000__3_E__http___purl_org_dc_elements_1_1_creator_
                  val partition = queryIndex(id)
                  //partition.foreach(p => println("p:"+p))
                  //println("--> Num of Partitions: "+partition.length)

                  val tableName = "tab" + id //tab0: triple pattern "0"
                  val subQuery = translate(partition, tp) + " AS " + tableName  //(SELECT ...WHERE partition)AS tab1
                  //println("2--> subQuery: ")

                  if(joinTables.contains(variable))  //[variable, (tab0, tab2)]
                    joinTables = joinTables + (variable -> (joinTables(variable) ++ Array(tableName)))
                  else
                    joinTables = joinTables + (variable -> Array(tableName))

                  if(!usedIds.contains(id)) {
                    varTableMap = varTableMap ++ extractVarTableMappings(subQuery, tableName)
                    //varTableMap.foreach(v => println(">> "+v))
                    selectVariables = selectVariables ++ findTPVars(tp)
                    subQueries = subQueries :+ subQuery
                    usedIds = usedIds :+ id
                  }

                }

            }}
            i+=1
        }}
        var finalQuery = ""
        var joinCondition = " WHERE "
        //val rootTable = joinTables(0) + "." + variable
        finalQuery+= "SELECT " + selectVariables.map(v => varTableMap(v) + "." + v + " AS " + v)   //tab0.type AS type
                                    .mkString(",") + " FROM "
        subQueries/*.reverse*/.foreach(q => {  //remove for removing query reordering
            finalQuery = finalQuery + q + ", "
        })
        //subQueries.foreach(q => println(q))

        finalQuery = finalQuery.patch(finalQuery.lastIndexOf(","), "", 2)
        //println("1 - finalQuery:  "+finalQuery)
        
        joinTables.foreach{case(v, tables) => { //[variable, (tab0, tab2)]
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

        //println("2 - finalQuery:  "+finalQuery)

        return queryMap + ("X" -> finalQuery)
    }

    def extractVarTableMappings(query: String, table: String): Map[String, String] = {
        val vars = query.substring(query.indexOf("SELECT") + 6, query.indexOf("FROM"))
        var varTableMap = Map[String, String]()
        vars.split(",").foreach(v => {
            val cleanV = v.substring(v.indexOf("AS")+2).trim
            varTableMap = varTableMap + (cleanV -> table)
        })

        //varTableMap.foreach(x => println(x))
        return varTableMap
    }

    // partition: table00000__3_E__http___purl_org_dc_elements_1_1_creator_   -- tp: (s,p,o)
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

  def translate(partition: Array[String], tp: (String, String, String)): String = {
    import spark.implicits._
    numberOfVars(tp) match {
      case 1 => singleVarTPSql(partition, tp)
      case 2 => doubleVarTPSql(partition, tp)
      case _ => threeVarTPSql(partition, tp)
    }
  }


  def singleVarTPSql(partition: Array[String], tp: Tuple3[String, String, String]): String = {
        val (s, p, o) = tp
        var query: String = ""
        if(s.contains("?")){//todo: NOT ever in??
            val v = QueryParser.removeNonAlphaNum(s)
            query = "(SELECT s AS " + v + " FROM " + partition + " WHERE o == '" + o + "')"
        }
        else if(p.contains("?")){
            val v = QueryParser.removeNonAlphaNum(p)
            query = "( "
            partition.foreach { p =>
              val part = p.replace("-", "_").replace("=", "_E_").trim // table00000__3_E__http___purl_org_dc_elements_1_1_creator_
              query = query + " (SELECT '<" + p + ">' AS " + v + " FROM " + "table" + part + " WHERE " + "s == '" + s + "' AND o == '" + o + "') " + "UNION"
            }
            //partition.foreach(p => println("?p => "+p))
            query = query.dropRight(5) + " )"
        }
        else{//todo: NOT ever in??
            val v = QueryParser.removeNonAlphaNum(o)
            query = "(SELECT o AS " + v + " FROM " + partition + " WHERE s == '" + s + "')"
        }
        return query
    }

    def doubleVarTPSql(partition: Array[String], tp: Tuple3[String, String, String]): String = {
        val (s, p, o) = tp
        var query: String = ""

        if(s.contains("?") && p.contains("?")) {
            val v1 = QueryParser.removeNonAlphaNum(s)
            val v2 = QueryParser.removeNonAlphaNum(p)
            query = "( "
            partition.foreach { p =>
              val part = p.replace("-", "_").replace("=", "_E_").trim // table00000__3_E__http___purl_org_dc_elements_1_1_creator_
              query = query + "(SELECT s AS " + v1 + ", '<" + p + ">' AS " + v2 + " FROM " + "table" + part + " WHERE " + "o == '" + o + "')" + "UNION"
            }
            //partition.foreach(p => println("?s,?p => "+p))
            query = query.dropRight(5) + " )"
        }
        else if(p.contains("?") && o.contains("?")) {
            val v1 = QueryParser.removeNonAlphaNum(p)
            val v2 = QueryParser.removeNonAlphaNum(o)
            query = "( "
            partition.foreach { p =>
              val part = p.replace("-", "_").replace("=", "_E_").trim // table00000__3_E__http___purl_org_dc_elements_1_1_creator_
              query = query + "(SELECT o AS " + v2 + ", '<" + p + ">' AS " + v1 + " FROM " + "table" + part + " WHERE " + "s == '" + s + "')" + "UNION"
            }
            //println( s+" - "+v1 + " - " + v2 +" : "+query)
            //partition.foreach(p => println("?p,?o => "+p))
            query = query.dropRight(5) + " )"
        }
        else {
            val v1 = QueryParser.removeNonAlphaNum(s)
            val v2 = QueryParser.removeNonAlphaNum(o)
            query = "(SELECT s AS " + v1 + ", o AS " + v2 + " FROM " + partition + ")"
        }
        return query
    }

    def threeVarTPSql(partition: Array[String], tp: Tuple3[String, String, String]): String = {
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
