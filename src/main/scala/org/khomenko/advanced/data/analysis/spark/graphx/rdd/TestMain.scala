package org.khomenko.advanced.data.analysis.spark.graphx.rdd

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}

object TestMain extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("graphTest")
    .getOrCreate()

  val vertices = spark.sparkContext.parallelize(
    Seq(
      (0L, Vertex(VertexType.Disease, "Dis1")),
      (1L, Vertex(VertexType.Disease, "Dis2")),
      (2L, Vertex(VertexType.Drug, "Dr1")),
      (3L, Vertex(VertexType.Drug, "Dr2")),
      (4L, Vertex(VertexType.Drug, "Dr3")),
      (5L, Vertex(VertexType.Drug, "Dr4")),
      (6L, Vertex(VertexType.Drug, "Dr5")),
      (7L, Vertex(VertexType.Disease, "Dis3"))
    )
  )
  val edges = spark.sparkContext.parallelize(
    Seq(
      Edge(2L, 3L, 2),
      Edge(3L, 2L, 2),
      Edge(0L, 2L, 1),
      Edge(0L, 5L, 1),
      Edge(0L, 4L, 1),
      Edge(1L, 4L, 1),
      Edge(1L, 6L, 1),
      Edge(1L, 3L, 1),
      Edge(7L, 6L, 1)
    )
  )

  val graph = Graph[Vertex, Int](vertices, edges)
  Utils.saveToGraphML(vertices, edges, "test-graph.graphml")

  val pw = new PrintWriter(new File("log.txt"))
  pw.println(s"Edges: ${graph.edges.count()}")
  pw.println(s"Vertices: ${graph.vertices.count()}")
  pw.println(s"Connected components: ${graph.connectedComponents(1000).vertices.map { case (_, cc) => cc }.distinct.count()}")

  val diseases = Seq(0L, 1L, 7L)
  val initialGraph = graph.mapVertices((id, v) => if (diseases contains id) v.copy(1) else v)
  val result = initialGraph.pregel(0, activeDirection = EdgeDirection.Either, maxIterations = 60)(
    (id, v, msg) => {
      v.vertexType match {
        case VertexType.Drug => v.copy(msg)
        case _ => v
      }
    },
    triplet => {
      if (triplet.srcAttr.vertexType == VertexType.Disease &&
        triplet.srcAttr.value == 1 &&
        triplet.dstAttr.vertexType == VertexType.Drug) {

        Iterator((triplet.dstId, 1))
      } else if (triplet.srcAttr.vertexType == VertexType.Drug &&
        triplet.dstAttr.vertexType == VertexType.Drug) {

        Iterator((triplet.dstId, 2))
      }
      else {
        Iterator.empty
      }
    },
    (a, b) => a + b
  )

  val resultVertices = result.vertices
    .filter(t => t._2.vertexType == VertexType.Drug && t._2.value == 3)
    .collect()

  pw.println(s"Arr size is: ${resultVertices.size}")

  resultVertices.foreach(t => {
    pw.println(s"${t._1}, ${t._2}, ${t._2.value}")
  })

  pw.close()
}
