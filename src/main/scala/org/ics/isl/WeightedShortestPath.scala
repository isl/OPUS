package org.ics.isl

import scala.reflect.ClassTag
import org.apache.spark.graphx._
/**
 * Computes weighted shortest paths to the given set of landmark vertices, returning a graph where each
 * vertex attribute is a map containing the shortest-path distance to each reachable landmark.
 * Currently supports only Graph of [VD, Double], where VD is an arbitrary vertex type.
 */
object WeightedShortestPath extends Serializable {
 
    //Initialize Map [ Centroid_Vertex_Id, (Sum(BC/CC, Path_Triples)) ] 
    type SPMap = Map[VertexId, Tuple2[Double, Seq[Tuple3[Long, Long, String]]]]

    // initial and infinity values, use to relax edges
    private val INITIAL = 0.0.toDouble
    private val INFINITY = Double.MaxValue

    private def makeMap(x: (VertexId,  Tuple2[Double, Seq[Tuple3[Long, Long, String]]])*) = Map(x: _*)

    private def incrementMap(spmap: SPMap, delta: Tuple4[Double, String, Long, Long]): SPMap = {
        val tuple = (delta._3, delta._4, delta._2)
        spmap.map{ case (v, d) => v -> Tuple2(d._1 + delta._1, d._2 :+ tuple)}
    }

    private def addMaps(spmap1: SPMap, spmap2: SPMap): SPMap = {  
        (spmap1.keySet ++ spmap2.keySet).map {
            k => (

                if(getFirst(spmap1.get(k)) < getFirst(spmap2.get(k))) 
                    k -> spmap1.get(k).getOrElse((Double.MaxValue, Seq[(Long, Long, String)]()))
                else 
                    k -> spmap2.get(k).getOrElse((Double.MaxValue, Seq[(Long, Long, String)]()))
            )
        }.toMap
    }
    
    // at this point it does not really matter what vertex type is
    def run[VD](graph: Graph[VD, (String, Double)], landmarks: Seq[VertexId]): Graph[SPMap,  (String, Double)] = {
        val spGraph = graph.mapVertices { 
            (vid, attr) => (
                if (landmarks.contains(vid)) makeMap(vid -> Tuple2(INITIAL, Seq[Tuple3[Long, Long, String]]())) 
                else makeMap()
            )
        }

        val initialMessage = makeMap()

        def vertexProgram(id: VertexId, attr: SPMap, msg: SPMap): SPMap = {
            addMaps(attr, msg)
        }

        def sendMessage(edge: EdgeTriplet[SPMap, (String, Double)]): Iterator[(VertexId, SPMap)] = {
            val newAttr = incrementMap(edge.dstAttr, (edge.attr._2, edge.attr._1, edge.srcId, edge.dstId))
            if (edge.srcAttr != addMaps(newAttr, edge.srcAttr)) Iterator((edge.srcId, newAttr))
            else Iterator.empty
        }

        Pregel(spGraph, initialMessage, Int.MaxValue, EdgeDirection.Either)(vertexProgram _, sendMessage _, addMaps _)
    }

    def getFirst(x: Option[Any]) = x match {
        case Some(y:Tuple2[_, _]) => getConstant(y._1)
        case _  => Double.MaxValue
    }

    def getConstant(x: Any) = x match {
        case f: Double => f
        case _ => Double.MaxValue
    }

    def getSeq(x: Any) = x match {
        case s: Seq[(Long, Long, String)] => s
        case _ => Seq[(Long, Long, String)]()
    }

    def getValue(x: Option[Any]) = x match {
        case Some(y: Tuple2[_, _]) => (getConstant(y._1), getSeq(y._2))
        case _  => (Double.MaxValue, Seq[(Long, Long, String)]())
    }
}