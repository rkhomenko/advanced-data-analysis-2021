package org.khomenko.advanced.data.analysis.spark.graphx.rdd

object VertexType extends Enumeration {
  val Disease, Drug = Value
}

case class Vertex(vertexType: VertexType.Value, code: String)
