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
import sys.process._

object Preprocessor {
    def initializeGraphs(spark: SparkSession) = {
        val sc = spark.sparkContext
        if(!HdfsUtils.hdfsExists(Constants.schemaVerticesFile))
           createSchemaGraph(spark)  
        if(!HdfsUtils.hdfsExists(Constants.instanceVerticesFile))
            createInstanceGraph3(spark)
    }

    def computeMeasures(sc: SparkContext, instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String], schemaGraph: Graph[String, (String, Double)]) = {
        val metaPath = Constants.huaBCFile.substring(0, Constants.huaBCFile.lastIndexOf("/"))
        
        if(!new File(metaPath).exists) {
            val mkdir = "mkdir " + metaPath !
        }
        if(!HdfsUtils.hdfsExists(Constants.schemaCC))
            computeCC(instanceGraph)
        if(!new File(Constants.huaBCFile).exists)    
            computeHuaBC(schemaGraph)
        if(!new File(Constants.schemaNodeFrequency).exists)        
            computeSchemaNodeFreq(instanceGraph)
        if(!new File(Constants.schemaImportance).exists)        
            computeNodeImportance(sc, schemaGraph)
        if(!new File(Constants.schemaNodeCount).exists)        
            computeSchemaNodeCount(instanceGraph)
    }

	def createSchemaGraph(spark: SparkSession) = {
        import spark.implicits._
        val sc = spark.sparkContext
        val schemaRdd = if(Constants.schemaPath.contains("lubm")){
            sc.textFile(Constants.schemaPath).filter(!_.startsWith("#"))
				.map(x => x.split("\\s+", 3).map(toUri(_)))
                .map(x => (x(0), (x(1), x(2))))
                //.filter{case(s,(p, o)) => s.contains("http") && o.contains("http")}
                .distinct.cache
        } 
        else {
            sc.textFile(Constants.schemaPath).filter(!_.startsWith("#"))
                .map(x => x.split("\\s+", 3).map(toUri(_)))
                .map(x => (x(0), (x(1), x(2))))
                .distinct.cache
        }
       
		val vertexRdd = schemaRdd.flatMap{case(s, (p, o)) => Seq(s,o)}
                                    .distinct
                                    .zipWithIndex
                                    .cache
        val verticeMap = vertexRdd.collectAsMap
        val brMap = sc.broadcast(verticeMap)
        val edges = schemaRdd.map{case(s, (p, o)) => Types.SchemaEdge(brMap.value(s), brMap.value(o), (p, 0.0))}
                                .distinct
                                
        val edgeDs = edges.toDF.as[Types.SchemaEdge]
        val vertexDs = vertexRdd.map{case(uri, id) => Types.SchemaVertex(id, uri)}.toDF.as[Types.SchemaVertex]

        edgeDs.write.format("parquet").save(Constants.HDFS + Constants.schemaEdgesFile)
        vertexDs.write.format("parquet").save(Constants.HDFS + Constants.schemaVerticesFile)

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
    def saveVerticesToFile(spark: SparkSession) = {
        val sc = spark.sparkContext    
        val instanceRdd =  sc.textFile(Constants.instancePath)
                                .filter(!_.startsWith("#"))
                                .map(_.split("\\s+", 3))
                                .map(t => (t(0), t(1), t(2).dropRight(2)))
                                .filter{case(s, p, o) => validTriple(s, p, o)}
                                .distinct

        val vertexRdd = instanceRdd.flatMap{case(s, p, o) => Seq(s, o)}
                                    .distinct
                                    .zipWithIndex
                                    .map{case(uri, id) => id + "\t" + uri}

        vertexRdd.saveAsTextFile(Constants.HDFS + "zip_vertices")


    }

    def isLiteral(str: String): Boolean = {str.startsWith("\"")}
    
    def createInstanceGraph3(spark: SparkSession) = {
        import spark.implicits._
        val sc = spark.sparkContext    
        val instanceRdd =  sc.textFile(Constants.instancePath)
                                .filter(!_.startsWith("#"))
                                .map(_.split("\\s+", 3))
                                .map(t => (t(0).trim, t(1).trim, t(2).dropRight(2).trim))
                               // .distinct
        
        val typedVertices = instanceRdd.flatMap{case(s, p, o) => {
            if(p == Constants.RDF_TYPE)
                Seq((s , Seq(("rdf:type", o))))
            else if(isLiteral(o))
                Seq((s , Seq((p, o))))
            else
                Seq((s, Seq()), (o, Seq()))
        }}

        val vertexRdd = typedVertices.reduceByKey(_++_)
                                        .zipWithIndex
                                        .map{case((uri, labels), id) => {
                                            (uri, (labels.filter(_._1 == "rdf:type").map(_._2),
                                                    labels.filter(_._1 != "rdf:type"),
                                                    id)
                                            )}
                                        }

        val filteredInstances = instanceRdd.map{case(s, p, o) => (s, (p, o))}
                                            .filter{case(s, (p, o)) => p != Constants.RDF_TYPE && !isLiteral(o)}
        val numPartitions = vertexRdd.partitions.size
        val vertices = vertexRdd.map{case(uri, (types, labels, id)) => (uri, id)}.repartition(numPartitions*2)
        
        val edgeSubjIds = filteredInstances.join(vertices).map{case(s, ((p, o), sId)) => (o, (p, sId))}
        val edges = edgeSubjIds.join(vertices).map{case(o, ((p, sId), oId)) => (sId, oId, p)}.distinct
        
        val edgeDs = edges.filter{case(s, o, p) => s != -1L && o != -1L}.map{case(s, o, p) => Types.InstanceEdge(s, o, p)}.toDF.as[Types.InstanceEdge]
        val vertexDs = vertexRdd.map{case(uri, (types, labels, id)) => Types.InstanceVertex(id, (uri, types, labels))}.toDF.as[Types.InstanceVertex]

        edgeDs.write.format("parquet").save(Constants.HDFS + Constants.instanceEdgesFile)
        vertexDs.write.format("parquet").save(Constants.HDFS + Constants.instanceVerticesFile)
    }

    // def createInstanceGraph(spark: SparkSession) = {
    //     import spark.implicits._
    //     val sc = spark.sparkContext    
    //     val instanceRdd =  sc.textFile(Constants.instancePath)
    //                             .filter(!_.startsWith("#"))
    //                             .map(_.split("\\s+", 3))
    //                             .map(t => (t(0), t(1), t(2).dropRight(2)))
    //                             .filter{case(s, p, o) => validTriple(s, p, o)}
    //                             .map(t => createTriple(t))
    //                             .distinct
        
    //     val typedVertices = instanceRdd.flatMap{case(s, o, p) => {
    //         if(p == Constants.RDF_TYPE)
    //             Seq((s , Seq(o._1)))
    //         else
    //             Seq((s, Seq()), (o, Seq()))
    //     }}

    //     val vertexRdd = typedVertices.reduceByKey(_++_)
    //                                  .map{case((uri, id), types) => (id, (uri, types))}

    //     val edgeRdd = instanceRdd.filter{case(s, o, p) => p != Constants.RDF_TYPE}
    //                                 .map{case((s, sId), (o, oId), p) => (sId, oId, p)}

    //     // val filteredInstances = instanceRdd.filter{case(s, p, o) => p != Constants.RDF_TYPE}
    //     // val edges = filteredInstances.map{case(s, p, o) => (uriHash(s), uriHash(o), p)}
        
    //     val edgeDs = edgeRdd.map{case(s, o, p) => Types.InstanceEdge(s, o, p)}.toDF.as[Types.InstanceEdge]                                   
    //     val vertexDs = vertexRdd.map{case(id, (uri, types)) => Types.InstanceVertex(id, (uri, types))}.toDF.as[Types.InstanceVertex]

    //     edgeDs.write.format("parquet").save(Constants.HDFS + Constants.instanceEdgesFile)
    //     vertexDs.write.format("parquet").save(Constants.HDFS + Constants.instanceVerticesFile)
    // }

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

	// def createInstanceGraph2(spark: SparkSession) = {
 //        import spark.implicits._
 //        val sc = spark.sparkContext    
 //        val instanceRdd =  sc.textFile(Constants.instancePath)
 //                                .filter(!_.startsWith("#"))
 //                                .map(_.split("\\s+", 3))
 //                                .map(t => (t(0), (t(1), t(2).dropRight(2))))
 //                                .filter{case(s, (p, o)) => validTriple(s, p, o)}
 //                                .distinct
 //        instanceRdd.cache       
        
 //        val typedVertices = instanceRdd.flatMap{case(s, (p, o)) => {
 //            if(p == Constants.RDF_TYPE)
 //                Seq((s , Seq(o)))
 //            else
 //                Seq((s, Seq()), (o, Seq()))
 //        }}
        
 //        val vertexRdd = typedVertices.reduceByKey(_++_)
 //                                    .zipWithIndex
 //                                    .map{case((node, types), id) => (node, (types, id))}
      

 //        val filteredInstances = instanceRdd.filter{case(s, (p, o)) => p != Constants.RDF_TYPE}
 //        val numPartitions = vertexRdd.partitions.size
 //        val vertices = vertexRdd.map{case(node, (types, id)) => (node, id)}.repartition(numPartitions*3)

 //        // val edgeSubjIds = filteredInstances.join(vertices).map{case(s, ((p, o), sId)) => (o, (p, sId))}
 //        // val edges = edgeSubjIds.join(vertices).map{case(o, ((p, sId), oId)) => (sId, oId, p)}

 //        // val verticeMap = vertices.collectAsMap
 //        // val brMap = sc.broadcast(verticeMap)
 //        // val edges = instanceRdd.map{case(s, (p, o)) => (brMap.value(s), brMap.value(o), p)}
 //        //                         .distinct
        

 //        val edges = createEdgeRdd(sc, vertices, instanceRdd.map(x => (x._1, x._2._1, x._2._2)))
 //        val edgeDs = edges.map{case(s, o, p) => Types.InstanceEdge(s, o, p)}.toDF.as[Types.InstanceEdge]                                   
 //        val vertexDs = vertexRdd.map{case(s, (types, id)) => Types.InstanceVertex(id, (s, types))}.toDF.as[Types.InstanceVertex]

 //        edgeDs.write.format("parquet").save(Constants.HDFS + Constants.instanceEdgesFile)
 //        vertexDs.write.format("parquet").save(Constants.HDFS + Constants.instanceVerticesFile)

 //        vertexRdd.unpersist()        
	// }

    def testGraphGeneration(sc: SparkContext) = {
        val instances = Seq(("jagathan" ,"firstname" ,"\"john\""), ("jagathan" ,"lastname" ,"\"agathan\""), 
                            ("uoc" ,"type" ,"univerity"), ("uoc" ,"place" ,"crete"), ("uoc" ,"property" ,"\"qwdioqweiqwd" +
                            "IOUO0I [    qwqwpdokq1`34214r2142132`   2 2  ete\""))

        val instanceRdd = sc.parallelize(instances).map{case(s, p ,o) => (s, (p, o))}      
        val vertexRdd = instanceRdd.flatMap{case(s, (p ,o)) => Seq(s, o)}.distinct.zipWithIndex
        println("Vertices")
        vertexRdd.collect.foreach(println)

        val edgeSubjIds = instanceRdd.join(vertexRdd).map{case(s, ((p, o), sId)) => (o, (p, sId))}
        val edges = edgeSubjIds.join(vertexRdd).map{case(o, ((p, sId), oId)) => (sId, p, oId)}
        println("Joins")
        edges.collect.foreach(println)

        val vertexMap = vertexRdd.collectAsMap
        val edges1 = instanceRdd.map{case (s, (p, o)) => (vertexMap(s), vertexMap(o), p)}
        println("Maps")
        edges1.collect.foreach(println)

    }


    def createEdgeRdd(sc: SparkContext, vertexRdd: RDD[(String, Long)], edgeRdd: RDD[(String, String, String)]): RDD[(Long, Long, String)] = {
        val vPartitions = vertexRdd.partitions
        var edges: RDD[((String, Long), (String, Long), String)] = edgeRdd.map{case(s, p, o) => ((s, -1L), (o, -1L), p)}
        var edgeRddArray = new Array[RDD[((String, Long), (String, Long), String)]](vPartitions.size)
        //var bcArray = new Array(org.apache.spark.broadcast.Broadcast[scala.collection.immutable.Map[String, Long]])(vPartitions.size)
        var i = 0
        for(p <- vPartitions) {
            val start = System.nanoTime()    
            val idx = p.index
            println(idx)
            val partRdd = vertexRdd.mapPartitionsWithIndex((index: Int, it: Iterator[(String, Long)]) => {
                if(index == idx)
                    it
                else
                    Iterator()
            })
            val vertexMap = partRdd.collectAsMap
            //bcArray(i) = sc.broadcast(vertexMap)
            if(i == 0) {
                edgeRddArray(i) = edges.map{case((s, sId), (o, oId), p) => {
                    ((s, if(sId == -1L) vertexMap.getOrElse(s, -1L) else sId),
                    (o, if(oId == -1L) vertexMap.getOrElse(o, -1L) else oId),
                    p)
                }}    
            }
            else {
                edgeRddArray(i) = edgeRddArray(i-1).map{case((s, sId), (o, oId), p) => {
                    ((s, if(sId == -1L) vertexMap.getOrElse(s, -1L) else sId),
                    (o, if(oId == -1L) vertexMap.getOrElse(o, -1L) else oId),
                    p)
                }}   
            }

            // if(i == vPartitions.size/2) {
            //     edgeRddArray(i).count
            //     bcArray.foreach(bc => {
            //         bc.unpersist()
            //         bc.destroy()
            //     })
            // }
            
            i+=1
            val end = System.nanoTime()    
            println(Duration((end - start), NANOSECONDS).toMillis)
        }
         edgeRddArray(i-1).map{case((s, sId), (o, oId), p) => (sId, oId, p)}
    }

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

    def computeCC(instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String]) = {       
        val classRDD = instanceGraph.triplets
                                    .filter(triplet => (!triplet.srcAttr._2.isEmpty && 
                                                        !triplet.dstAttr._2.isEmpty)) //contain class
                                    .flatMap(triplet => 
                                            combineTripleTypes((triplet.srcAttr._2, 
                                                        triplet.attr, 
                                                        triplet.dstAttr._2, 
                                                        Seq((triplet.srcAttr._1, triplet.dstAttr._1))
                                            ))
                                    )

        val combinedClassRDD = classRDD.filter{
                                            case((s, p, o), ids) => 
                                                (!s.isEmpty && !p.isEmpty && !o.isEmpty)
                                        }.reduceByKey((a,b) => a ++ b)
        
        combinedClassRDD.map{case(classes, instances) => 
                                (classes, instances.size, instances.map(_._2).distinct.size)}
                         .saveAsTextFile(Constants.HDFS + Constants.schemaCC)   
    }

    def computeNodeImportance(sc: SparkContext, schemaGraph: Graph[String, (String, Double)]) = {
        val nodeFreq: Map[String, Int] = Loader.loadSchemaNodeFreq()
        val brNodeFreq = sc.broadcast(nodeFreq)

        val schemaFreq:Map[String, Int] = schemaGraph.vertices
                                                        .map(v => (v._2, brNodeFreq.value.getOrElse(v._2, 0)))
                                                        .collectAsMap  
    	val bcMap: Map[String, Double] = Loader.loadBC()

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
        val pw = new PrintWriter(new File(Constants.schemaImportance))
        ListMap(importance.toSeq.sortWith(_._2 > _._2):_*).foreach{case(uri, impValue) => {
           //if condition for dbpedia dataset
            if(Constants.schemaImportance.contains("dbpedia")){
                if(uri.contains(Constants.dbpediaUri))
                    pw.write(schemaMap(uri) + "\t" + impValue + "\n")
                else
                    pw.write(schemaMap(uri) + "\t0.0001\n")
            }
            else {
                pw.write(schemaMap(uri) + "\t" + impValue + "\n")
            }
        }}
        pw.close
    }

    def computeSchemaNodeFreq(instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String]) = {
        val schemaNodeFreq = instanceGraph.triplets.filter(triplet => (!triplet.srcAttr._2.isEmpty || 
                                                                            !triplet.dstAttr._2.isEmpty))
                                            .flatMap(triplet => Seq(triplet.srcAttr, triplet.dstAttr))
                                            .filter(node => !node._2.isEmpty)
                                            .distinct
                                            .flatMap(triplet => (triplet._2))
                                            .map(c => (c, 1))
                                            .reduceByKey(_+_)
        val pw = new PrintWriter(new File(Constants.schemaNodeFrequency))
        schemaNodeFreq.sortBy(_._2, false).collect.foreach{case (uri, freq) => {
            pw.write(uri + "\t" + freq + "\n")
        }}
        pw.close
    }

    def computeSchemaNodeCount(instanceGraph: Graph[(String, Seq[String], Seq[(String, String)]), String]) = {
        val schemaNodeFreq = instanceGraph.triplets.filter(triplet => (!triplet.srcAttr._2.isEmpty || 
                                                                            !triplet.dstAttr._2.isEmpty))
                                            .flatMap(triplet => triplet.srcAttr._2  ++ triplet.dstAttr._2)
                                            .map(c => (c, 1))
                                            .reduceByKey(_+_)
        val pw = new PrintWriter(new File(Constants.schemaNodeCount))
        schemaNodeFreq.sortBy(_._2, false).collect.foreach{case (uri, freq) => {
            pw.write(uri + "\t" + freq + "\n")
        }}
        pw.close
    }

    def saveWeightedGraph(vertices: RDD[(Long, String)], edges: EdgeRDD[(String, Double)], k: Int) = {
        vertices.saveAsTextFile(Constants.HDFS + Constants.weightedVertices + "_" + k)
        edges.map(e => (e.srcId, e.dstId, e.attr)).saveAsTextFile(Constants.HDFS + Constants.weightedEdges + "_" + k)
    }

    
    def saveShortestPathVertices(rdd: RDD[(Long, scala.collection.immutable.Map[VertexId, Tuple2[Double, Seq[Tuple3[Long, Long, String]]]])], k: Int) = {
        val temp = rdd.map{case(id, m) => Types.ShortestPath(id, m)}
        KryoFile.saveAsObjectFile(temp, Constants.HDFS + Constants.shortestPaths + "_" + k)
    }
    /**
    * Produce all possible combinations of subject and object types.
    */
    def combineTripleTypes(tuple: (Seq[String], String, Seq[String], Seq[(String, String)])): Seq[((String, String, String), Seq[(String, String)])] = {
        //produce combinations of lists subjectType, objType
        for {
            subjectType <- tuple._1
            objType <- tuple._3
        } yield((subjectType, tuple._2, objType), tuple._4)
    }    

    def cleanTripleLubm(triple: Array[String]) = {
          if(triple(2).startsWith("\""))
              (triple(0).drop(1).dropRight(1), (triple(1).drop(1).dropRight(1), triple.drop(2).mkString.dropRight(1)))
          else
              (triple(0).drop(1).dropRight(1), (triple(1).drop(1).dropRight(1), triple(2).drop(1).dropRight(1)))
    }

  	def computeHuaBC(graph: Graph[String, (String, Double)]) = {
      	val verticeMap = graph.vertices.collectAsMap
  		val bcMap = org.ics.isl.betweenness.HuaBC.computeBC(graph)
          				  .sortBy(_._2, false)
          				  .map(x => (x._1, verticeMap(x._1), x._2))
          				  .collect
        val pw = new PrintWriter(new File(Constants.huaBCFile))
        bcMap.foreach{case(id, uri, bcValue) => {
            pw.write(id + "\t" + uri + "\t" + bcValue + "\n")
        }}
        pw.close
    }

    def normalizeValue(value: Double, min: Double, max: Double) = (value - min) / (max - min)
	
	def computeEdmondsBC(graph: Graph[String, (String, Double)], k: Int) = {
		val verticeMap = graph.vertices.collectAsMap
		val bcMap = org.ics.isl.betweenness.EdmondsBC.computeBC(graph)
    					.sortBy(_._2, false)
    					.map(x => (x._1, verticeMap(x._1), x._2))
                        .collect

        val pw = new PrintWriter(new File(Constants.edmondsBCFile + "_" + k))
        bcMap.foreach{case(id, uri, bcValue) => {
            pw.write(id + "\t" + uri + "\t" + bcValue + "\n")
        }}
        pw.close
    }
}