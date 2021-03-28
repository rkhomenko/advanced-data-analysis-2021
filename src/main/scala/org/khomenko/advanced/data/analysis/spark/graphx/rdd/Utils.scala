package org.khomenko.advanced.data.analysis.spark.graphx.rdd

import java.io.{File, PrintWriter}
import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, monotonically_increasing_id}

object Utils {

  private def loadDisease2DrugGraph(path: String, spark: SparkSession)
               : (RDD[(Long, Vertex)], RDD[Edge[Int]], Dataset[Row]) = {
    val df = spark.read.format("csv")
      .option("sep", "\t")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(path)

    val verticesDf = df.select(df("Disease")).distinct()
      .withColumnRenamed("Disease", "code")
      .withColumn("type", lit("disease"))
      .union(df.select(df("Drug")).distinct()
        .withColumnRenamed("Drug", "code")
        .withColumn("type", lit("drug")))
      .withColumn("id", monotonically_increasing_id())

    val diseases = verticesDf.select(verticesDf("id"), verticesDf("code"))
      .where(verticesDf("type") === "disease")

    val drugs = verticesDf.select(verticesDf("id"), verticesDf("code"))
      .where(verticesDf("type") === "drug")

    val edges = df.as("df")
      .join(diseases.as("dis"), df("Disease") === diseases("code"), "right")
      .join(drugs.as("drs"), col("df.Drug") === col("drs.code"), "right")
      .select(col("dis.id").alias("dis_id"), col("drs.id").alias("drs_id"))
      .rdd
      .map(row => Edge(row.getAs[Long]("dis_id"), row.getAs[Long]("drs_id"), 1))

    val vertices = verticesDf
      .rdd
      .map(row => row.getAs[String]("type") match {
        case "drug" => (row.getAs[Long]("id"), Vertex(
          VertexType.Drug,
          row.getAs[String]("code")
        ))
        case "disease" => (row.getAs[Long]("id"), Vertex(
          VertexType.Disease,
          row.getAs[String]("code")
        ))
      })

    (vertices, edges, drugs)
  }

  private def loadDrug2DrugGraph(path: String, drugs: Dataset[Row], spark: SparkSession): RDD[Edge[Int]] = {
    val df = spark.read.format("csv")
      .option("sep", "\t")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(path)

    df.as("df")
      .join(drugs.as("drgs1"), col("df.Drug1") === col("drgs1.code"), "inner")
      .join(drugs.as("drgs2"), col("df.Drug2") === col("drgs2.code"), "inner")
      .select(col("drgs1.id").alias("left_id"), col("drgs2.id").alias("right_id"))
      .rdd
      .map(row => Edge(row.getAs[Long]("left_id"), row.getAs[Long]("right_id"), 2))
  }

  def loadGraph(disease2DrugPath: String, drug2DrugPath: String, spark: SparkSession)
               : (RDD[(Long, Vertex)], RDD[Edge[Int]]) = {
    val (firstNodes, firstEdges, drugs) = loadDisease2DrugGraph(disease2DrugPath, spark)
    val secondEdges = loadDrug2DrugGraph(drug2DrugPath, drugs, spark)

    (firstNodes, firstEdges.union(secondEdges))
  }

  def saveToGraphML(edges: RDD[Edge[Int]], vertices: RDD[(Long, Vertex)], path: String): Unit = {
    val begin =
      """<?xml version="1.0" encoding="UTF-8"?>
        |<graphml xmlns="http://graphml.graphdrawing.org/xmlns"
        |xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        |xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns
        |http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">
        |<key attr.name="label" attr.type="string" for="node" id="label"/>
        |<key attr.name="weight" attr.type="int" for="edge" id="weight"/>
        |<key attr.name="r" attr.type="int" for="node" id="r"/>
        |<key attr.name="g" attr.type="int" for="node" id="g"/>
        |<key attr.name="b" attr.type="int" for="node" id="b"/>
        |<graph id="G" edgedefault="undirected">""".stripMargin
    val end =
      """</graph>
        |</graphml>""".stripMargin

    val red =
      """<data key="r">255</data>
        |<data key="g">0</data>
        |<data key="b">0</data>
        |""".stripMargin

    val blue =
      """<data key="r">0</data>
        |<data key="g">0</data>
        |<data key="b">255</data>
        |""".stripMargin

    val nodes = vertices
      .map(t => {
        val color = t._2.vertexType match {
          case VertexType.Drug => red
          case VertexType.Disease => blue
        }

        s"""<node id="n${t._1}">
           |<data key="label">${t._2}</data>
           |${color}
           |</node>""".stripMargin
      })
      .collect()

    val eds = edges
      .zipWithUniqueId()
      .map((t) => {
        s"""<edge id="e${t._2}" source="n${t._1.srcId}" target="n${t._1.dstId}">
           |<data key="weight">${t._1.attr}</data>
           |</edge>""".stripMargin
      })
      .collect()

    val pw = new PrintWriter(new File(path))
    pw.println(begin)
    nodes.foreach(s => pw.println(s))
    eds.foreach(s => pw.println(s))
    pw.println(end)
    pw.close()
  }
}
