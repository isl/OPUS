 package org.ics.isl

import org.apache.spark.graphx._

import scala.concurrent.duration._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.util.Random
import scala.collection.Map
import scala.collection.immutable.ListMap
import java.io._

import org.apache.spark.sql.functions.countDistinct

import sys.process._

object Preprocessor {

    def initializeGraphs(spark: SparkSession, dataset: String, hdfs: String, schemaPath: String, instancePath: String) = {
        val sc = spark.sparkContext
      if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.schemaVerticesFile))
        createSchemaGraph(spark, dataset, hdfs, schemaPath)
      if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.instanceVerticesFile))
        createInstanceGraph(spark, dataset, hdfs, instancePath)
    }

    def computeMeasures(spark: SparkSession, sc: SparkContext, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String], schemaGraph: Graph[String, (String, Double)], dataset: String, hdfs: String) = {
      val metaPath = dataset + Constants.huaBCFile.substring(0, Constants.huaBCFile.lastIndexOf("/"))


      if(!new File(metaPath).exists) {
            val mkdir = "mkdir " + metaPath !
        }
      if(!HdfsUtils.hdfsExists(hdfs + dataset + Constants.schemaCC))
            computeCC(spark, instanceGraph,dataset, hdfs)
        if(!new File(dataset + Constants.huaBCFile).exists)    
            computeHuaBC(schemaGraph, dataset)
        if(!new File(dataset + Constants.schemaNodeFrequency).exists)        
            computeSchemaNodeFreq(instanceGraph, dataset)
        if(!new File(dataset + Constants.schemaImportance).exists)        
            computeNodeImportance(sc, schemaGraph, dataset)
        if(!new File(dataset + Constants.schemaNodeCount).exists)        
            computeSchemaNodeCount(instanceGraph, dataset)
    }

	def createSchemaGraph(spark: SparkSession, dataset: String, hdfs: String, schemaPath: String) = {
        import spark.implicits._
        val sc = spark.sparkContext
        val schemaRdd = if(dataset.contains("lubm")){
            sc.textFile(schemaPath).filter(!_.startsWith("#"))
				.map(x => x.split("\\s+", 3).map(toUri(_)))
                .map(x => (x(0), (x(1), x(2))))
                //.filter{case(s,(p, o)) => s.contains("http") && o.contains("http")}
                .distinct.cache
        } 
        else {
            sc.textFile(schemaPath).filter(!_.startsWith("#"))
                .map(x => x.split("\\s+", 3).map(toUri(_)))
                .map(x => (x(0), (x(1), x(2))))
                .distinct.cache
        }
       
		    val vertexRdd = schemaRdd.flatMap{case(s, (p, o)) => Seq(s,o)} //(s,o,s,o...)
                                    .distinct
                                    .zipWithIndex
                                    .sortByKey()
                                    .cache   //(vertex, id)
        val verticeMap = vertexRdd.collectAsMap
        val brMap = sc.broadcast(verticeMap)
        val edges = schemaRdd.map{case(s, (p, o)) => Types.SchemaEdge(brMap.value(s), brMap.value(o), (p, 0.0))} //brMap.value(s): id sub
                                .distinct
                                
        val edgeDs = edges.toDF.as[Types.SchemaEdge]
        val vertexDs = vertexRdd.map{case(uri, id) => Types.SchemaVertex(id, uri)}.toDF.as[Types.SchemaVertex]


        edgeDs.write.format("parquet").save(hdfs + dataset + Constants.schemaEdgesFile)
        vertexDs.write.format("parquet").save(hdfs + dataset + Constants.schemaVerticesFile)

        schemaRdd.unpersist()
        vertexRdd.unpersist()		
	}

    def toUri(uri: String): String = {"<" + uri + ">"}

    //LUBM helper
    def propertyHelper(property: String): Boolean = {
        val properties = Seq("inverseOf", "subPropertyOf")
        properties.foreach(p => {
            if(property.contains(p)){
                return false
            }
        })
        return true
    }

    //LUBM helper
    def nodeHelper(node: String): Boolean = {
        val nodes = Seq("ObjectProperty", "TransitiveProperty", "DatatypeProperty", "Class" ,"Restriction")
        nodes.foreach(n => {
            if(node.contains(n)){
                return false
            }
            if(!node.contains("http")){
                return false
            }
        })
        return true
    }
   
    def isLiteral(str: String): Boolean = {str.startsWith("\"")}
    
    def createInstanceGraph(spark: SparkSession, dataset: String, hdfs: String, instancePath: String) = {
        import spark.implicits._
        val sc = spark.sparkContext    
        val instanceRdd =  sc.textFile(instancePath)
                                .filter(!_.startsWith("#"))
                                .map(_.split("\\s+", 3))
                                .map(t => (t(0).trim, t(1).trim, t(2).dropRight(2).trim)) //(s, p, o)


        //SOS: DELETE
        //instanceRdd.toDF.write.format("parquet").save(hdfs + dataset + "_data/instance_triples")

        //println("$$$$$$$$")
        //println(instanceRdd.first())
        //println("$$$$$$$$")
        val typedVertices = instanceRdd.flatMap{case(s, p, o) => {
            if(p == Constants.RDF_TYPE)
                Seq((s , Seq(("rdf:type", o))))
            else if(isLiteral(o))
                Seq((s , Seq((p, o))))
            else
                Seq((s, Seq()), (o, Seq()))
        }}

        //_++_: concatenates  arguments
        //_._1 take the fisrt element of a tuple
        val vertexRdd = typedVertices.reduceByKey(_++_).sortByKey()
                                        .zipWithIndex
                                        .map{case((uri, labels), id) => {
                                            (uri, (labels.filter(_._1 == "rdf:type").map(_._2),
                                                    labels.filter(_._1 != "rdf:type"),
                                                    id)
                                            )}
                                        } //(uri, (types, labels, id))
        //vertexRdd: (uri, (types, labels(p,literal), id))
        //vertices: (uri, id) --  all vertices at instances
        //filteredInstances: (s, (p, o)), where p != Constants.RDF_TYPE && !isLiteral(o)]
        val filteredInstances = instanceRdd.map{case(s, p, o) => (s, (p, o))}
                                            .filter{case(s, (p, o)) => p != Constants.RDF_TYPE && !isLiteral(o)}
        val numPartitions = vertexRdd.partitions.size
        val vertices = vertexRdd.map{case(uri, (types, labels, id)) => (uri,id)}.repartition(numPartitions*2)

       // vertices.take(5).foreach{case(uri,id) => println("vertice_5:    "+uri+ " : "+id)}
        //val nodes_Ma = vertices.filter(_._1 == "<http://data.semanticweb.org/conference/eswc/2006/talks/paper-presentation-ontology-evaluation-ma>")
        //nodes_Ma.collect().foreach{case(uri,id) => println("vertices_MA:    "+uri+ " : "+id)}


        //val nodes_un = vertices.filter(_._1 == "<http://dx.doi.org/10.1007/978-3-642-35173-0_5>")
        //nodes_un.collect().foreach{case(uri,id) => println("vertices_UN:    "+uri+ " : "+id)}

        val edgeSubjIds = filteredInstances.join(vertices).map{case(s, ((p, o), sId)) => (o, (p, sId))}
        val edges = edgeSubjIds.join(vertices).map{case(o, ((p, sId), oId)) => (sId, oId, p)}.distinct //edges:  (sId, oId, p)

        //val edges_Ma = edges.filter{case(sId, oId, p) => p == "http://data.semanticweb.org/ns/swc/ontology#isRoleAt"}.map{{case(sId, oId, p) => (sId)}
       // val edges_Ma_1 = edges.filter{case(sId, oId, p) => sId == 1243}
        //edges_Ma_1.collect().foreach{case(sId, oId, p) => println("edges_MA_34:    " + sId + " " + p + " " +oId)}

        //val edges_Ma_2 = edges.filter{case(sId, oId, p) => sId == 31513}
        //edges_Ma_2.collect().foreach{case(sId, oId, p) => println("edges_MA_35:    " + sId + " " + p + " " +oId)}




        //edges_Ma.foreach{case(sId, oId, p) => println("node_MA:    "+uri+ " : "+id)}
        
        val edgeDs = edges.filter{case(s, o, p) => s != -1L && o != -1L}.map{case(s, o, p) => Types.InstanceEdge(s, o, p)}.toDF.as[Types.InstanceEdge]
        val vertexDs = vertexRdd.map{case(uri, (types, labels, id)) => Types.InstanceVertex(id, (uri, types, labels))}.toDF.as[Types.InstanceVertex]

        //vertexRdd.filter(x => x._2._3 == 1243).collect().foreach(t=> println("--vertexRDD_34: "+t._1))
        //vertexRdd.filter(x => x._2._3 == 31513).collect().foreach(t=> println("--vertexRDD_35: "+t._1))

        //vertexDs.filter(x => x.id == 1243).collect().foreach(t => println("-- vertexDS_34: "+t.data._1))
        //vertexDs.filter(x => x.id == 31513).collect().foreach(t => println("-- vertexDS_35: "+t.data._1))

        edgeDs.write.format("parquet").save(hdfs + dataset + Constants.instanceEdgesFile)
        vertexDs.write.format("parquet").save(hdfs + dataset + Constants.instanceVerticesFile)
    }


    def createTriple(triple: (String, String, String)): ((String, Long), (String, Long), String) = {
        val (s, p, o) = triple
        if(o.startsWith("\"")){
            ((s, uriHash(s)), (o, literalHash(o)), p)
        }
        else {
            ((s, uriHash(s)), (o, uriHash(o)), p)
        }
    }

    def uriHash(uri: String): Long = {uri.toLowerCase.hashCode.toLong}

    def literalHash(literal: String): Long = {Random.nextLong()}

    def validTriple(s: String, p: String, o: String): Boolean = {
        !p.contains("wikiPageWikiLink")
    }
    
	def getValue(a: Option[Any]): Seq[String] = {
        a match {
          case Some(x: Seq[_]) => x.map(_.toString) 
          case _ => Seq[String]()
        }
    }

    def cleanTriple(t: Array[String], delim: String): (String, String, String) = {
        val s = if(t(0).startsWith("<")) t(0).replaceAll("[<>]", "") else t(0)
        var p = if(t(1).startsWith("<")) t(1).replaceAll("[<>]", "") else t(1)
        var o = if(t(2).startsWith("<")) 
                    t(2).replaceAll("[<>]", "").replace(" .", "")
                else 
                    t.drop(2).mkString(delim).dropRight(2)
        return (s, p, o)
    }

    def computeCC(spark: SparkSession, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String],dataset: String, hdfs: String) = {
        import spark.implicits._
        val classRDD = instanceGraph.triplets
          .filter(triplet => (!triplet.srcAttr._2.isEmpty &&
                              !triplet.dstAttr._2.isEmpty)) //contain class
          .flatMap(triplet =>
              combineTripleTypes((triplet.srcAttr._2,
                triplet.attr,
                triplet.dstAttr._2,
                triplet.srcAttr._1, triplet.dstAttr._1))
              )
          .filter{case((s, p, o, inst1, inst2)) => (!s.isEmpty && !p.isEmpty && !o.isEmpty)}

        val classDF = classRDD.toDF("subType", "pred", "objType", "inst1","inst2")
        val classGroupBy = classDF.groupBy("subType", "pred", "objType")

        val freq = classGroupBy.count()
        //println("freq.count:"+freq.count())
       // freq.printSchema()

        val distinctFreq = classGroupBy.agg(countDistinct("inst2"))
        val distinctFreqNew = distinctFreq.withColumnRenamed("count(inst2)", "dist")

        //println("distinct:"+distinctNew.count())
        //distinctFreq.printSchema()
        val combinedClassDF = freq.join(distinctFreqNew, freq.col("subType") === distinctFreqNew.col("subType") &&  freq.col("pred") === distinctFreqNew.col("pred") &&  freq.col("objType") === distinctFreqNew.col("objType") )//.where(count("subType") === distinct("subType")  &&  count("pred") === distinct("pred") &&  count("objType") === distinct("objType"))
       // println("combinedClassRDD join:"+combinedClassDF.count())
        //combinedClassDF.printSchema()
        //combinedClassDF.take(10).foreach(x => println("join: "+x.get(0) +" - " + x.get(1) +" - " + x.get(2) +" - " + x.getLong(3) +" - " + x.getLong(7)))

        //((<http://data.semanticweb.org/ns/swc/ontology#TutorialsChair>,<http://data.semanticweb.org/ns/swc/ontology#heldBy>,<http://xmlns.com/foaf/0.1/Person>),3,3)
        combinedClassDF.rdd.map{x =>( (x.getString(0), x.getString(1), x.getString(2)), x.getLong(3), x.getLong(7))}
          .saveAsTextFile(hdfs + dataset + Constants.schemaCC)

    }



    def computeNodeImportance(sc: SparkContext, schemaGraph: Graph[String, (String, Double)], dataset: String) = {
        val nodeFreq: Map[String, Int] = Loader.loadSchemaNodeFreq(dataset)
        val brNodeFreq = sc.broadcast(nodeFreq)

        val schemaFreq:Map[String, Int] = schemaGraph.vertices
                                                        .map(v => (v._2, brNodeFreq.value.getOrElse(v._2, 0)))
                                                        .collectAsMap  
    	val bcMap: Map[String, Double] = Loader.loadBC(dataset)

    	val bcMax = bcMap.valuesIterator.max
    	val bcMin = bcMap.valuesIterator.min

    	val freqMax = schemaFreq.valuesIterator.max
    	val freqMin = schemaFreq.valuesIterator.min

        val importance: Map[String, Double] = bcMap.map{case(uri, bcValue) => {
            val normBc = normalizeValue(bcValue, bcMin, bcMax)
            val normFreq = normalizeValue(schemaFreq(uri), freqMin, freqMax)
            (uri, normBc + normFreq)
        }}
        
        val schemaMap = schemaGraph.vertices.map(x => (x._2, x._1)).collectAsMap
        val pw = new PrintWriter(new File(dataset + Constants.schemaImportance))
        val pwURI = new PrintWriter(new File(dataset + Constants.schemaImportanceURI))
       // val pwTest = new PrintWriter(new File(dataset + "_local/schema_URInode.txt"))
        ListMap(importance.toSeq.sortWith(_._2 > _._2):_*).foreach{case(uri, impValue) => {
            pw.write(schemaMap(uri) + "\t" + impValue + "\n")
            pwURI.write(uri + "\t" + impValue + "\n")
           //if condition for dbpedia dataset
            /*if(Constants.schemaImportance.contains("dbpedia")){
                if(uri.contains(Constants.dbpediaUri))
                    pw.write(schemaMap(uri) + "\t" + impValue + "\n")
                else
                    pw.write(schemaMap(uri) + "\t0.0001\n")
            }
            else {
                pw.write(schemaMap(uri) + "\t" + impValue + "\n")
            }*/
        }}
        pw.close
        pwURI.close
        //pwTest.close
    }
    //instance graph:  val vertexDs = vertexRdd.map{case(uri, (types, labels, id)) => Types.InstanceVertex(id, (uri, types, labels))}.toDF.as[Types.InstanceVertex]
    def computeSchemaNodeFreq(instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String], dataset: String) = {
        val schemaNodeFreq = instanceGraph.triplets.filter(triplet => (!triplet.srcAttr._2.isEmpty || 
                                                                            !triplet.dstAttr._2.isEmpty))
                                            .flatMap(triplet => Seq(triplet.srcAttr, triplet.dstAttr))
                                            .filter(node => !node._2.isEmpty)
                                            .distinct
                                            .flatMap(triplet => (triplet._2))
                                            .map(c => (c, 1))
                                            .reduceByKey(_+_)
        val pw = new PrintWriter(new File(dataset + Constants.schemaNodeFrequency))
        schemaNodeFreq.sortBy(_._2, false).collect.foreach{case (uri, freq) => {
            pw.write(uri + "\t" + freq + "\n")
        }}
        pw.close
    }

    def computeSchemaNodeCount(instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String], dataset: String) = {
        val schemaNodeFreq = instanceGraph.triplets.filter(triplet => (!triplet.srcAttr._2.isEmpty || 
                                                                            !triplet.dstAttr._2.isEmpty))
                                            .flatMap(triplet => triplet.srcAttr._2  ++ triplet.dstAttr._2)
                                            .map(c => (c, 1.toLong))
                                            .reduceByKey(_+_)
        val pw = new PrintWriter(new File(dataset + Constants.schemaNodeCount))
        schemaNodeFreq.sortBy(_._2, false).collect.foreach{case (uri, freq) => {
            pw.write(uri + "\t" + freq + "\n")
        }}
        pw.close
    }

    def saveWeightedGraph(vertices: RDD[(Long, String)], edges: EdgeRDD[(String, Double)], k: Int, dataset: String, hdfs: String) = {
        vertices.saveAsTextFile(hdfs + dataset + Constants.weightedVertices + "_" + k)
        edges.map(e => (e.srcId, e.dstId, e.attr)).saveAsTextFile(hdfs + dataset + Constants.weightedEdges + "_" + k)
    }

    
    def saveShortestPathVertices(rdd: RDD[(Long, scala.collection.immutable.Map[VertexId, Tuple2[Double, Seq[Tuple3[Long, Long, String]]]])], k: Int, dataset: String, hdfs: String) = {
        val temp = rdd.map{case(id, m) => Types.ShortestPath(id, m)}
        KryoFile.saveAsObjectFile(temp, hdfs + dataset + Constants.shortestPaths + "_" + k)
    }
    /**
    * Produce all possible combinations of subject and object types.
    */

    /*def combineTripleTypes(tuple: (Seq[String], String, Seq[String], Seq[(String, String)])): Seq[((String, String, String), Seq[(String, String)])] = {*/
    def combineTripleTypes(tuple: (Seq[String], String, Seq[String], String, String)): Seq[(String, String, String, String, String)] = {
        //produce combinations of lists subjectType, objType
        /*for {
            subjectType <- tuple._1
            objType <- tuple._3
        } yield((subjectType, tuple._2, objType), tuple._4)*/
        for {
            subjectType <- tuple._1 //triplet.srcAttr._2: subjectType
            objType <- tuple._3 //triplet.dstAttr._2: objType
        } yield(subjectType, tuple._2, objType, tuple._4, tuple._5)
    }    

    def cleanTripleLubm(triple: Array[String]) = {
          if(triple(2).startsWith("\""))
              (triple(0).drop(1).dropRight(1), (triple(1).drop(1).dropRight(1), triple.drop(2).mkString.dropRight(1)))
          else
              (triple(0).drop(1).dropRight(1), (triple(1).drop(1).dropRight(1), triple(2).drop(1).dropRight(1)))
    }

  	def computeHuaBC(graph: Graph[String, (String, Double)], dataset: String) = {
      	val verticeMap = graph.vertices.collectAsMap
  		val bcMap = org.ics.isl.betweenness.HuaBC.computeBC(graph)
          				  .sortBy(_._2, false)
          				  .map(x => (x._1, verticeMap(x._1), x._2))
          				  .collect
        val pw = new PrintWriter(new File(dataset + Constants.huaBCFile))
        bcMap.foreach{case(id, uri, bcValue) => {
            pw.write(id + "\t" + uri + "\t" + bcValue + "\n")
        }}
        pw.close
    }

    def normalizeValue(value: Double, min: Double, max: Double) = (value - min) / (max - min)
	
	def computeEdmondsBC(graph: Graph[String, (String, Double)], k: Int, dataset: String) = {
		val verticeMap = graph.vertices.collectAsMap
		val bcMap = org.ics.isl.betweenness.EdmondsBC.computeBC(graph)
    					.sortBy(_._2, false)
    					.map(x => (x._1, verticeMap(x._1), x._2))
                        .collect

        val pw = new PrintWriter(new File(dataset + Constants.edmondsBCFile + "_" + k))
        bcMap.foreach{case(id, uri, bcValue) => {
            pw.write(id + "\t" + uri + "\t" + bcValue + "\n")
        }}
        pw.close
    }
}