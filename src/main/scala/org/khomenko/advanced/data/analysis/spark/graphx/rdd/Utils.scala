package org.khomenko.advanced.data.analysis.spark.graphx.rdd

import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lit, row_number}

object Utils {
  def loadGraph(path: String, spark: SparkSession): (RDD[(Long, Vertex)], RDD[Edge[Int]]) = {
    val drug = "drug"
    val disease = "disease"

    val df = spark.read.format("csv")
      .option("sep", "\t")
      .option("inferSchema", "true")
      .option("header", "true")
      .load(path)
    val diseases = df.select(df("Disease")).distinct()
      .withColumn("dis_id", row_number().over(Window.orderBy("Disease")))

    val drugs = df.select(df("Drug")).distinct()
      .withColumn("drug_id", row_number().over(Window.orderBy("Drug")))

    val edges = df.as("df").join(diseases.as("dis"), df("Disease") === diseases("Disease"), "right")
      .join(drugs.as("drs"), df("Drug") === drugs("Drug"), "right")
      .select("dis.dis_id", "drs.drug_id")
      .rdd
      .map(row => Edge(row.getInt(0), row.getInt(1), 1))

    val vertices = diseases
      .withColumnRenamed("Disease", "code")
      .withColumnRenamed("dis_id", "id")
      .withColumn("type", lit(disease))
      .union(
        drugs
          .withColumnRenamed("Drug", "code")
          .withColumnRenamed("drug_id", "id")
          .withColumn("type", lit(drug))
      )
      .rdd
      .map(row => row.getString(2) match {
        case "drug" => (row.getInt(1).toLong, Vertex(VertexType.Drug, row.getString(0)))
        case "disease" => (row.getInt(1).toLong, Vertex(VertexType.Disease, row.getString(0)))
      })

    (vertices, edges)
  }
}
