package org.khomenko.advanced.data.analysis.spark.graphx.rdd

import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.io.{File, PrintWriter}

object TestMain extends App {
  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("graphTest")
    .getOrCreate()

  import spark.implicits._

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

  val verticesDF = vertices.map(i => {
    (i._1, i._2.vertexType.toString, i._2.code)
  }).toDF("id", "type", "code")
  val edgesDF = edges.map(e => (e.srcId, e.dstId, e.attr))
    .toDF("srcId", "destId", "attr")
  edgesDF
    .join(verticesDF.as("vSrc"), $"vSrc.id" === $"srcId")
    .join(verticesDF.as("vDest"), $"vDest.id" === $"destId")
    .select(
      col("vSrc.id").alias("srcId"),
      col("vSrc.type").alias("srcType"),
      col("vSrc.code").alias("srcCode"),
      col("vDest.id").alias("destId"),
      col("vDest.type").alias("destType"),
      col("vDest.code").alias("destCode"),
    )
    .write
    .format("org.neo4j.spark.DataSource")
    .mode(SaveMode.Overwrite)
    .option("authentication.basic.username", "neo4j")
    .option("authentication.basic.password", "neo4j1")
    .option("url", "bolt://localhost:7687")
    .option("relationship", "EDGE")
    .option("relationship.save.strategy", "keys")
    .option("relationship.source.labels", ":Vertex")
    .option("relationship.source.save.mode", "overwrite")
    .option("relationship.source.node.keys", "srcId:id")
    .option("relationship.source.node.properties", "srcType:type,srcCode:code")
    .option("relationship.target.labels", ":Vertex")
    .option("relationship.target.node.keys", "destId:id")
    .option("relationship.target.node.properties", "destType:type,destCode:code")
    .option("relationship.target.save.mode", "overwrite")
    .option("schema.optimization.type", "NODE_CONSTRAINTS")
    .save()

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
