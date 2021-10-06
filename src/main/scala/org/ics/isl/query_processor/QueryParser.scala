package org.ics.isl

import scala.io.Source
import java.io.File
import scala.collection.mutable.Map
import org.apache.spark.sql._


object QueryParser {
    var file: File = null
	var query: String = null
	var variables: Array[String] = null
	var triplePatterns: Array[(Int, (String, String, String))] = null
	var isIndexedQuery: Boolean = false
	var queryMappings: Map[Int, String] = null


	def parseQuery(queryFile: File) = {
        file = queryFile
        query = Source.fromFile(queryFile).getLines.mkString
        variables = extractSelectVariables() // Array[String]
        triplePatterns = extractTriplePatterns().zipWithIndex.map(x => (x._2, x._1))
        
        val mappingResult = extractQueryMappings()
        isIndexedQuery = mappingResult._2
        queryMappings = mappingResult._1
	}

    def arrayEquals[A, B](a1: Array[A], a2: Array[B]): Boolean = {a1.diff(a2).isEmpty && a2.diff(a1).isEmpty}

	def extractQueryVars(): Array[String] = {
		triplePatterns.flatMap{case(id, tp) => findTPVars(tp)}.distinct
	}
    
	/**
	* Extract select variables from query
	*/
	def extractSelectVariables(): Array[String] = {
		val selectPattern = "\\b(SELECT|select.*?(?=from|FROM))\\b".r
		val selectVariables = selectPattern.findFirstMatchIn(query).mkString
		//drop SELECT replace non-alphanumeric
        selectVariables.split(" ").drop(1).map(removeNonAlphaNum(_))
	}

	/**
	* Extract triple pattern from query
	*/
	def extractTriplePatterns(): Array[(String, String, String)] = {
        val pattern = "(\\{.*?\\})".r
		    //extract string of triple patterns
        val triplePatternStr = pattern.findAllMatchIn(query)
                                        .mkString
                                        .drop(1)
                                        .dropRight(1)
        
        if(triplePatternStr.isEmpty){
            isIndexedQuery = false
            return Array()
        }
        else{
            //extract triple patterns from query
            var triplePatterns = triplePatternStr.split(" \\.")
                                                    .filter(!_.startsWith("#"))
                                                    .map(x => cleanTp(x.trim.split("\\s+").map(_.trim)))
                                                    .map(x => {
                                                            //x.foreach{p => println(">> "+p)}
                                                            //println(x.mkString("-- "))
                                                            if(x(1) == "a" || x(1) == "rdf:type")
                                                                (x(0), Constants.RDF_TYPE, x(2))
                                                            else
                                                                (x(0), x(1), x(2))
                                                            })
            return triplePatterns
        }
	}

    /**
    * Extract mappings from query
    * Maps each tripple patten to a uri
    * Triple patterns that contain uri and queries that have an rdf:type
    */
    def extractQueryMappings(): (Map[Int, String], Boolean) = { //return tpMapping
        var tpMapping = Map[Int, String]() //(id -> uri) -- constant: uri_instance, variable: uri_Class (?x rdf:type Class)
        var rdfTypeMapping = Map[String, String]() //[variable , Class]

        triplePatterns.foreach{case(id, (s, p, o)) => {
            if(!isVariable(s) && !isLiteral(s)) { //constant: id->uri_s
                tpMapping = tpMapping + (id -> s)
            }
            if(!isVariable(o) && !isLiteral(o)) {//constant: id->uri_o  -- maybe subject is variable?
                tpMapping = tpMapping + (id -> o)
                val cleanPred = cleanTp(Array(p)).mkString //cleans triple pattern uris from < >
                if(cleanPred == Constants.RDF_TYPE) {
                    rdfTypeMapping = rdfTypeMapping + (s -> o) //?x(variable) -> uri_o_ClASS
                }
            }
        }}
        
        triplePatterns.foreach{case(id, (s, p , o)) => {
            if(isVariable(s) && isVariable(o)) {// variable ?s && ?o   //todo: if sub & obj are variables?
                if(rdfTypeMapping.contains(s)){
                    tpMapping = tpMapping + (id -> rdfTypeMapping(s))  //(id -> uri_Class_s)
                }
                if(rdfTypeMapping.contains(o)){
                    tpMapping = tpMapping + (id -> rdfTypeMapping(o))//(id -> uri_Class_o)
                }
            }
            else if(isVariable(s)) {// variable ?s
                if(rdfTypeMapping.contains(s) && p != Constants.RDF_TYPE) {
                    tpMapping = tpMapping + (id -> rdfTypeMapping(s)) //id->uri_class(s)
                }
            }
            else if(isVariable(o)) {// variable ?o
                if(rdfTypeMapping.contains(o)) {
                    tpMapping = tpMapping + (id -> rdfTypeMapping(o))//id->uri_class(0)
                }
            }
        }}
        
        if(arrayEquals(tpMapping.keys.toArray, triplePatterns.map(_._1))) //for each triple pattern of query exists an association: id->uri in tpMapping????
            return (tpMapping, true)
        else
            return (tpMapping, false)
    }

    def isVariable(str: String): Boolean = str.contains("?")

    def isLiteral(str: String): Boolean = str.contains("\"")

    //Helper Methods

    /**
    * cleans triple pattern uris from < >
    */
    def cleanTp(tp: Array[String]): Array[String] = {
        tp.map(t => {
            if(t.contains("\"") || isVariable(t)) {
                t 
            }
            else{ 
                if(!t.startsWith("<") && !t.endsWith("<")){
                    "<" + t + ">"
                }
                else if(!t.startsWith("<")) {
                    "<" + t
                }
                else if(!t.endsWith(">")) {
                    t + ">"
                }
                else {
                    t
                }
            }
        })
    }
    /**
    * removes every non alphanumeric char in string
    */
    def removeNonAlphaNum(str: String): String = {
    	str.replaceAll("[^a-zA-Z0-9]", "")
    }

    /**
    * tranforms map to mutable
    */
    def toMutable[A, B](map: scala.collection.immutable.Map[A, B]) = {scala.collection.mutable.Map() ++ map}

    /**
    * Finds variables in a triple patterm
    */
    def findTPVars(tp: Tuple3[String, String, String]): Array[String] = {
        tp.productIterator.zipWithIndex
                            .filter(_._1.toString.contains("?"))
                            .map(x =>  removeNonAlphaNum(x._1.toString))
                            .toArray
    }
}