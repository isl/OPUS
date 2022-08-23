package org.ics.isl

import java.io.{File, PrintWriter}
import sys.process._

import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.SparkSession

import scala.collection.Map
import scala.io.Source
//import org.apache.log4j.{Level, LogManager}
//import org.apache.spark.sql.functions._
//import org.apache.spark.sql.SparkSession
//
//import scala.collection.mutable
import scala.collection.mutable.HashMap

object QueryOptimizer {
  val spark = loadSparkSession()
  var dataset = ""
  var hdfs = ""
  var partitionNum = -1


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


  //troulin
  /**
   *  A function that optimizes a set of queries in a folder.
   * @param inputPath: folder path with the queries
   * @param inputMap: map that contains the statistics of sub-partition
   * @return: String with the FULLY OPTIMIZED WHERE statement
   */
  def Optimizer(inputPath: String, inputMap: Map[String, Double], optimizedFolder: String): Unit ={
    import spark.implicits._

    val fileList = listFiles(new File(inputPath), true)

    fileList.foreach(queryFile => {
        val queryPath = queryFile.getPath
        val queryName = queryFile.getName

        val pw = new PrintWriter(new File(optimizedFolder + queryName))

        //println("queryPath: "+queryPath)
        println("Optimize queryName: "+queryName)

        //Parse query
        val query = Source.fromFile(queryFile).mkString
        val optimizedQuery = QueryOptimizer(query, inputMap)

        pw.write(optimizedQuery)
        pw.close()

       println("Optimized Query: "+ optimizedQuery)


    })
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


  /**
   *  A function that implements the main Query Optimization logic.
   * @param fullMap: Hashmap with (keys,values). Keys are the statements in AS statements and values are a list with all the possible tabs for join
   * @param costMap: Hashmap with keys the tab names and values the corresponding join cost
   * @return: String with the FULLY OPTIMIZED WHERE statement
   */
  def QueryOptimizerLogic(fullMap: HashMap[String, Array[String]], costMap: HashMap[String, Double]): String ={

    /**
     * We could consider this problems as a graph problem, with the tables being the vertices and their joins as edges.
     * Essentially we want to find the Hamiltonian path https://www.hackerearth.com/practice/algorithms/graphs/hamiltonian-path/tutorial/
     * We also know that when there are N nodes, the minimum number of edges in a connected graph are n-1.
     * PROOF: https://www.quora.com/How-do-I-prove-that-the-minimum-number-of-edges-in-a-connected-graph-with-n-vertices-is-n-1
     * The final goal to achieve the best joins possible(with the least cost).
     */

    var bestComb : List[(String, Double, String, String)] = Nil // A list to preserve the best combinations


    //First iterate over the Hashmap (key,values). Values is a list that contains all the available tables between them
    //We are certain that there are no joins where only one table of a specific key exists
    for (dictIter <- fullMap; if dictIter._2.length > 1 ){

      var tempKeyComb : List[(String, Double, String, String)] = Nil  //List with all the combinations for table joins in each key

      for(innerList <- dictIter._2.indices){  //  Iterate over the list elements. innerList is an index value

        for(remElem <- innerList+1 until dictIter._2.length){   // Go from the next index to the end of the table to track all combinations
          val tempProd: Double = costMap(dictIter._2(innerList)) * costMap(dictIter._2(remElem))  // Find the cost of join between two arrays, by calculating the product
          val tempTuple: (String, Double, String, String) = (dictIter._2(innerList)+"."+dictIter._1+"="+dictIter._2(remElem)+"."+dictIter._1, tempProd, dictIter._2(innerList), dictIter._2(remElem))  // Create a temporary tuple with the where join statement and the cost

          tempKeyComb = tempTuple :: tempKeyComb
        }
      }

      var bestCombTables : List[String] = Nil // A list to preserve the best combinations
      val sortedtempComb : List[(String, Double, String, String)]= tempKeyComb.sortBy(_._2)  //Sort everything by the join cost(ascending)

      /*
          Keep the N - 1 elements that we want from each case, as stated in the beginning. NOTE N-1 from the initial list with the elements
       */

      var elemIter = 0
      var counter = 0
      while(elemIter < dictIter._2.length - 1){

        //If there is an element that already joins two tables that are joined move to the next element
        if(bestCombTables.contains(sortedtempComb(counter)._3) && bestCombTables.contains(sortedtempComb(counter)._4)){
          counter += 1
        }else{

          if (!bestCombTables.contains(sortedtempComb(counter)._3)){ //If an element does not exist in the tables already joined add it
            bestCombTables = sortedtempComb(counter)._3 :: bestCombTables
          }

          if(!bestCombTables.contains(sortedtempComb(counter)._4)){
            bestCombTables = sortedtempComb(counter)._4 :: bestCombTables
          }

          bestComb = sortedtempComb(counter) :: bestComb //If we reach here it means that at least one of the tables in the join is new
          elemIter += 1
          counter += 1
        }
      }

    }

    val sortedBestComb : List[String] = bestComb.sortBy(_._2).map(x => x._1) //Sort once again to get the final ordering, and keep the only the combinations
    val finalWhereQuery: String = " WHERE " +sortedBestComb.mkString(" AND ") //Final WHERE STATEMENT OPTIMIZED

    //println("Final WHERE STATEMENT "+ finalWhereQuery) //WHERE sanity check
    finalWhereQuery
  }


  def QueryOptimizer(inputQuery: String,  inputMap:Map[String,Double]) : String = {

    val newlineSplit: Array[String] = inputQuery.split("\\r?\\n")    //Split the input based on the newline character

    if (newlineSplit(newlineSplit.length - 1).split(" ")(1).toInt == 1 ) return inputQuery

    val query: String = newlineSplit(1) //Essentially we want to modify just the query(2nd entry), while the rest remain the same
    val _SelectQuery: String = StringUtils.substringBefore(query, "FROM") // Select statement will not change in the output

    //Extracts contents of the query from the first FROM statement until the last WHERE STATEMENT
    var initQueryDiv : String = StringUtils.substringAfter(query, "FROM")
    initQueryDiv = StringUtils.substringBeforeLast(initQueryDiv, "WHERE").replaceAll("\n", " ").trim

    //Regex to split string by commas that is not included inside the parentheses to get all the different tabs
    val tabSplitter: Array[String] = initQueryDiv.split(",\\s*(?![^()]*\\))")

    // Structure that contains an Array[(tab name, AS statements, full table name ,whole from statement)]
    val tableIdentifier: Array[(String, String, String, String)] = tabSplitter
      .map(x=>(StringUtils.substringAfterLast(x, "AS").trim, StringUtils.substringBetween(x, "SELECT", "FROM"),  if (StringUtils.substringAfter(x, "FROM").trim.split(" ")(0).endsWith(")")) StringUtils.substringAfter(x, "FROM").trim.split(" ")(0).dropRight(1) else StringUtils.substringAfter(x, "FROM").trim.split(" ")(0),x))


    //costMap is a Hashmap containing the tab name and the tuple count.
    //STATIC IMPLEMENTATION
    //    val costMap: HashMap[String, Double] = HashMap("tab0" -> 1379623, "tab1" -> 724685, "tab2" -> 309815, "tab3" -> 5580609, "tab4" -> 160140, "tab5" -> 1619476)

    //    Implementation using table name and count
    //    var costMap: HashMap[String, Double] = new HashMap()
    //    tableIdentifier.foreach(n => costMap.update(n._1, loadDataset(n._3).count()))


    //  Implementation reading statistics from file
   /* val inputMap:Map[String,Double] = scala.io.Source.fromFile(fileStatisticsPath).getLines.map {
      l =>
        l.split(' ') match {
          case Array(k, v, _*) => "table" + k.replace("-", "_").replace("=", "_E_").trim -> v.toDouble
          case _ => "-1.0" -> -1.0    //this is to get when there is an empty line that causes an exception otherwise
        }
    }.toMap.filter(x=> x._2 != -1.0 )   //Keep only the valid entries
    */

    var costMap: HashMap[String, Double] = new HashMap()
    tableIdentifier.map(n=> costMap.update(n._1, inputMap(n._3)))  //TODO maybe do something special when the stat does not exist from the file


    val finalTable: Array[(String,Double)] = tableIdentifier.map(t => (t._4, costMap.get(t._1) match {
      case Some(value) => value
    })).sortBy(_._2)    //Final sorted statements for the FROM statement

    val _FromQuery: String = "FROM "+finalTable.map(x => x._1).mkString(", ") //The final FROM part of the query

    val rgxAS = "AS\\s((\\w+))|as\\s(\\w+)".r //Regex to extract statements that start with AS or as
    var fullMap: HashMap[String, Array[String]] = new HashMap() //A hashmap with keys the statements stored in AS statements and values all the possible tabs for join

    tableIdentifier.foreach( x => {
      rgxAS.findAllIn(x._2).matchData.foreach(

        m => fullMap.get(m.group(1)) match {
          case None => fullMap.update(m.group(1), Array(x._1))  //In case the element does not exist initialize the array
          case Some(value) => fullMap.update(m.group(1), value :+ x._1) //Otherwise append to the list the new element
        }
      )
    })

    val _WhereQuery: String = QueryOptimizerLogic(fullMap, costMap) //Function that returns the optimized WHERE statement
    val _Query: String = _SelectQuery + _FromQuery + _WhereQuery

    newlineSplit(1) = _Query  //Change only the final query
    val finalOutput: String = newlineSplit.mkString("\n") //Merge once again the initially split string, with the newline character

    return finalOutput
  }


  def main(args: Array[String]): Unit = {
    import spark.implicits._

    if(args.size != 4) {
      println("-->Missing Arguments")
      System.exit(-1)
    }

    dataset = args(0)
    partitionNum = args(1).toInt
    hdfs = args(2)
    //path of folder of translated queries
    var inputPath = args(3)

    if(!hdfs.endsWith("/"))
      hdfs = hdfs + "/"


    //create optimized queries folder
    val optimizedFolder = inputPath + "/" + "optimizedTranslated_queries" + "_" + partitionNum + "_bal"

    if(!new File(optimizedFolder).exists) {
      val mkdir = "mkdir " + optimizedFolder !
    }
    val fullPath = optimizedFolder + "/"// + subDir + "/"
    if(!new File(fullPath).exists) {
      val mkdir = "mkdir " + fullPath !
    }


    //  Implementation reading statistics from file
   //todo:  inputMap = Loader.loadStatisticsTables(partitionNum, dataset)

   //Implementation reading statistics from file
    val inputMap:Map[String,Double] = scala.io.Source.fromFile(dataset + Constants.statTableFile).getLines.map {
      l =>
        l.split(' ') match {
          case Array(k, v, _*) => "table" + k.replace("-", "_").replace("=", "_E_").trim -> v.toDouble
          case _ => "-1.0" -> -1.0    //this is to get when there is an empty line that causes an exception otherwise
        }
    }.toMap.filter(x=> x._2 != -1.0 )   //Keep only the valid entries


    println("Size inputMap: "+inputMap.size)
    inputMap.foreach{println(_)}

    //val inputQuery = ">>>>> Q2.txt\nX SELECT tab5.Y AS Y,tab2.Z AS Z,tab3.X AS X FROM (SELECT s AS Y FROM table00001__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#University>') AS tab1, (SELECT s AS Z, o AS Y FROM table00001__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_suborganizationof_) AS tab4, (SELECT s AS X, o AS Y FROM table00001__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_undergraduatedegreefrom_) AS tab5, (SELECT s AS X FROM table00002__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#GraduateStudent>') AS tab0, (SELECT s AS X, o AS Z FROM table00004__3_E__http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_memberof_) AS tab3, (SELECT s AS Z FROM table00004__3_E__http___www_w3_org_1999_02_22_rdf_syntax_ns_type_ WHERE o == '<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#Department>') AS tab2 WHERE tab1.Y=tab4.Y AND tab4.Y=tab5.Y AND tab0.X=tab3.X AND tab3.X=tab5.X AND tab2.Z=tab3.Z AND tab3.Z=tab4.Z \npartitions 00002-_3=_http___www_w3_org_1999_02_22_rdf_syntax_ns_type_,00001-_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_undergraduatedegreefrom_,00001-_3=_http___www_w3_org_1999_02_22_rdf_syntax_ns_type_,00004-_3=_http___www_w3_org_1999_02_22_rdf_syntax_ns_type_,00004-_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_memberof_,00001-_3=_http___www_lehigh_edu__zhp2_2004_0401_univ_bench_owl_suborganizationof_\nTP 6"
    //println("Initial input is: \n\n"+inputQuery)

    Optimizer(inputPath, inputMap, fullPath)
    //val finOutput = QueryOptimizer(inputQuery, fileStatisticsPath)
    //println("\n\nOutput is: \n\n"+finOutput)
  }


}
