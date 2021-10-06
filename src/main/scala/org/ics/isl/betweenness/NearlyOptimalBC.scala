package org.ics.isl.betweenness

import org.apache.spark.graphx._

import scala.reflect.ClassTag

/**
 * Created by mth on 5/6/17.
 */
class NearlyOptimalBC[VD, ED: ClassTag](graph: Graph[VD, ED]) extends Serializable {

  private val nOBCProcessor = new NearlyOptimalBCProcessor[VD, ED](graph)

  def computeBC = {

    val initBFSGraph = nOBCProcessor.initGraph

    val sigmaGraph = Pregel[NOVertex, NOVertex, ED, List[NOMessage[VertexId]]](initBFSGraph,
      nOBCProcessor.prepareVertices(nOBCProcessor.initVertexId),
      nOBCProcessor.applyMessages,
      nOBCProcessor.sendMessages,
      _ ++ _, 2)

    val bcvector = sigmaGraph.vertices.mapValues(v => v.bc / 2)

    BetweennessUtils.normalize(bcvector, directed = false)
  }

}

object HuaBC extends Serializable {
  def computeBC[VD, ED: ClassTag](graph: Graph[VD, ED]) =
    new NearlyOptimalBC[VD, ED](graph).computeBC
}