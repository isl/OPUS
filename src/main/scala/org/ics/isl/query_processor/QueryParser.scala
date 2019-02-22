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
        variables = extractSelectVariables()
        triplePatterns = extractTriplePatterns().zipWithIndex.map(x => (x._2, x._1))
        
        val mappingResult = extractQueryMappings2()
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
        //extract triple patterns from query
        var triplePatterns = triplePatternStr.split(" \\.")
                                                .filter(!_.startsWith("#"))
                                                .map(x => cleanTp(x.trim.split("\\s+").map(_.trim)))
                                                .map(x => {
                                                        if(x(1) == "a" || x(1) == "rdf:type")
                                                            (x(0), Constants.RDF_TYPE, x(2))
                                                        else
                                                            (x(0), x(1), x(2))
                                                        })
        triplePatterns
	}

    /**
    * Extract mappings from query
    * Maps each tripple patten to a uri
    * Triple patterns that contain uri and queries that have an rdf:type
    */
    def extractQueryMappings2(): (Map[Int, String], Boolean) = {
        var tpMapping = Map[Int, String]()
        var rdfTypeMapping = Map[String, String]()

        triplePatterns.foreach{case(id, (s, p, o)) => {
            if(!isVariable(s) && !isLiteral(s)) {
                tpMapping = tpMapping + (id -> s)
            }
            if(!isVariable(o) && !isLiteral(o)) {
                tpMapping = tpMapping + (id -> o)
                val cleanPred = cleanTp(Array(p)).mkString
                if(cleanPred == Constants.RDF_TYPE) {
                    rdfTypeMapping = rdfTypeMapping + (s -> o)
                }
            }
        }}
        
        triplePatterns.foreach{case(id, (s, p , o)) => {
            if(isVariable(s) && isVariable(o)) {
                if(rdfTypeMapping.contains(s)){
                    tpMapping = tpMapping + (id -> rdfTypeMapping(s))
                }
                if(rdfTypeMapping.contains(o)){
                    tpMapping = tpMapping + (id -> rdfTypeMapping(o))
                }
            }
            else if(isVariable(s)) {
                if(rdfTypeMapping.contains(s) && p != Constants.RDF_TYPE) {
                    tpMapping = tpMapping + (id -> rdfTypeMapping(s))
                }
            }
            else if(isVariable(o)) {
                if(rdfTypeMapping.contains(o)) {
                    tpMapping = tpMapping + (id -> rdfTypeMapping(o))
                }
            }
        }}
        
        if(arrayEquals(tpMapping.keys.toArray, triplePatterns.map(_._1)))
            return (tpMapping, true)
        else
            return (tpMapping, false)
    }

    // def extractQueryMappings(): (Map[Int, (String, String)], Boolean) = {
    //     var tpMapping = Map[Int, (String, String)]()
    //     var initialTpMapping = Map[Int, (String, String)]()

    //     var initialVariableMapping = Map[String, String]()
    //     var variableMapping = Map[String, String]()

    //     triplePatterns.foreach{case(id, (s, p, o)) => {
    //         if(!isVariable(s) && !isLiteral(s)) {
    //             val (tpMap, varMap) = fillMaps(s, (s, p, o), id, initialTpMapping, initialVariableMapping, "subj")
    //             initialTpMapping = tpMap
    //             initialVariableMapping = varMap
    //         }
    //         if(!isVariable(o) && !isLiteral(o)) {
    //             val (tpMap, varMap) = fillMaps(o, (s, p, o), id, initialTpMapping, initialVariableMapping, "obj")
    //             initialTpMapping = tpMap
    //             initialVariableMapping = varMap
    //         }
    //     }}
        
    //     triplePatterns.foreach{case(id, tp) => {
    //         val tpVars = findTPVars(tp)
    //         var varUri = ""
    //         var tempVar = ""
    //         tpVars.foreach(v => {
    //             if(initialVariableMapping.contains(v)){
    //                 tempVar = v
    //                 varUri = initialVariableMapping(v)
    //             }
    //         })
    //         if(varUri != "") {
    //             if(!initialTpMapping.contains(id)){
    //                 tpMapping = tpMapping + (id -> (varUri, findPosition(tp, tempVar)))
    //             }
    //         }            
    //     }}

    //     tpMapping = tpMapping ++ initialTpMapping
       
    //     if(arrayEquals(tpMapping.keys.toArray, triplePatterns.map(_._1)))
    //         return (tpMapping, true)
    //     else
    //         return (tpMapping, false)
    // }

    def fillMaps(uri: String, tp: (String, String, String), id: Int, tpMapping: Map[Int, String], variableMapping: Map[String, String]) = {
        var varMap = variableMapping
        var tpMap = tpMapping
        val tpVars = findTPVars(tp)
        var flag = 0
        tpVars.foreach(v => {
            if(!varMap.contains(v)){
                varMap = varMap + (removeNonAlphaNum(v) -> uri)
                flag = 1
            }
        })
        if(flag == 0)
            tpMap = tpMap + (id -> varMap(tpVars(0)))
        else
            tpMap = tpMap + (id -> uri)
        (tpMap, varMap)
    }

    def findPosition(tp: (String, String, String), variable: String): String = {
        if(removeNonAlphaNum(tp._1) == variable)
            return "subj"
        else
            return "obj"
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