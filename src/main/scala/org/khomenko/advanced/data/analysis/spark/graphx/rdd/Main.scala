package org.khomenko.advanced.data.analysis.spark.graphx.rdd

import java.io.{File, PrintWriter}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph}
import Utils.{loadGraph, processRawTSV, saveGraph, saveToGraphML}

object Main extends App {
  val loadFromTSV = args.length match {
    case 4 => true
    case _ => false
  }

  if (args.length < 2) {
    println("No args provided")
    System.exit(-1)
  }

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("graphTest")
    .getOrCreate()

  val graph: Graph[Vertex, Int] = if (loadFromTSV) {
    val (verticesRDD, edgesRDD) = processRawTSV(args(0), args(1), spark)
    val graph: Graph[Vertex, Int] = Graph(verticesRDD, edgesRDD)

    saveToGraphML(verticesRDD, edgesRDD, "graph.graphml")
    saveGraph(verticesRDD, edgesRDD, args(2), args(3), spark)

    graph
  } else {
    loadGraph(args(0), args(1), spark)
  }

  val pw = new PrintWriter(new File("log.txt"))
  pw.println(s"Edges: ${graph.edges.count()}")
  pw.println(s"Vertices: ${graph.vertices.count()}")
  pw.println(s"Connected components: ${graph.connectedComponents(1000).vertices.map { case (_, cc) => cc }.distinct.count()}")
  pw.close()

//  val diseases = graph.triplets
//    .filter(t => t.srcAttr.vertexType == VertexType.Disease && t.dstAttr.vertexType == VertexType.Drug)
//    .map(t => t.srcId)
//    .collect()
//    .toSeq
//  val initialGraph = graph.mapVertices((id, v) => if (diseases contains id) v.copy(1) else v)
//  val result = initialGraph.pregel(0, activeDirection = EdgeDirection.Either)(
//    (id, v, msg) => {
//      v.vertexType match {
//        case VertexType.Drug => v.copy(msg)
//        case _ => v
//      }
//    },
//    triplet => {
//      if (triplet.srcAttr.vertexType == VertexType.Disease &&
//        triplet.srcAttr.value == 1 &&
//        triplet.dstAttr.vertexType == VertexType.Drug) {
//
//        Iterator((triplet.dstId, 1))
//      } else if (triplet.srcAttr.vertexType == VertexType.Drug &&
//        triplet.dstAttr.vertexType == VertexType.Drug &&
//        triplet.dstAttr.value == 1) {
//
//        Iterator((triplet.dstId, 2))
//      }
//      else {
//        Iterator.empty
//      }
//    },
//    (a, b) => math.max(a, b)
//  )
}
