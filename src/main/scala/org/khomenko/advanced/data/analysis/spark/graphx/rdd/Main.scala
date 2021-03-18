package org.khomenko.advanced.data.analysis.spark.graphx.rdd

import org.apache.spark.sql.SparkSession
import Utils.loadGraph
import org.apache.spark.graphx.Graph

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
  graph.inDegrees.foreach(println)
}
