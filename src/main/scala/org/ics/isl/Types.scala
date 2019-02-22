package org.ics.isl
import org.apache.spark.graphx._


object Types {
	
	case class InstanceVertex(id: VertexId,	data: Tuple3[String, Seq[String], Seq[(String, String)]])
 	case class InstanceEdge(src: VertexId, dst: VertexId, e: String)
 	
    case class SchemaVertex(id: VertexId, uri: String)
    case class SchemaEdge(src: VertexId, dst: VertexId, e: Tuple2[String, Double])  

 	case class Index(uri: String, clusters: Seq[String])
 	case class NodeIndex(uri: String, uriType: String)
 	case class ClassIndex(uri: String, clusters: Seq[String])

    case class CardinalityInstances(triple: Tuple3[String, String, String], instanceList: Seq[Tuple2[VertexId, VertexId]])
    case class CardinalityInstances2(triple: Tuple3[String, String, String], instanceList: Seq[Tuple2[String, String]])
    case class CardinalityValues(triple: Tuple3[String, String, String], size: Int, distinctSize: Int)
    case class ShortestPath(id: VertexId, map: Map[VertexId, Tuple2[Double, Seq[Tuple3[VertexId, VertexId, String]]]])
    case class Betweenness(id: VertexId, uri: String, bcValue: Double)
}
