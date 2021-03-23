package org.khomenko.advanced.data.analysis.spark.graphx.rdd

import java.io.{File, PrintWriter}

import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx.Graph

import Utils.{loadGraph, saveToGraphML}

object Main extends App {
  if (args.length < 1) {
    println("Path to tsv needed")
    System.exit(-1)
  }

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("graphTest")
    .getOrCreate()

  val (verticesRDD, edgesRDD) = loadGraph(args(0), spark)
  val graph: Graph[Vertex, Int] = Graph(verticesRDD, edgesRDD)

  val pw = new PrintWriter(new File("log.txt" ))
  pw.println(s"Edges: ${graph.edges.count()}")
  pw.println(s"Vertices: ${graph.vertices.count()}")
  pw.println(s"Connected components: ${graph.connectedComponents(1000).vertices.map{ case(_,cc) => cc}.distinct.count()}")
  pw.close()

  saveToGraphML(edgesRDD, verticesRDD, "graph.graphml")
}
