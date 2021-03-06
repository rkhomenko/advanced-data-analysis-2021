package org.khomenko.advanced.data.analysis.spark.graphx.rdd

object VertexType extends Enumeration {
  val Disease, Drug = Value
}

case class Vertex(vertexType: VertexType.Value, code: String, value: Int = 0) {
  override def toString: String = s"${vertexType} ${code}"

  def copy(newValue: Int): Vertex = {
    Vertex(vertexType, code, newValue)
  }
}
