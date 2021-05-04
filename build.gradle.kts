plugins {
    scala
    `java-library`
}

repositories {
    mavenCentral()

    flatDir {
        dirs("libs")
    }
}

val scalaVersion = "2.12"
val sparkVersion = "3.1.1"

dependencies {
    implementation(group = "org.scala-lang", name = "scala-library", version = "2.12.13")
    compileOnly(group = "org.apache.spark", name = "spark-core_$scalaVersion", version = sparkVersion)
    compileOnly(group = "org.apache.spark", name = "spark-sql_$scalaVersion", version = sparkVersion)
    compileOnly(group = "org.apache.spark", name = "spark-graphx_$scalaVersion", version = sparkVersion)
    compileOnly(group = "neo4j-contrib", name = "neo4j-connector-apache-spark_2.12", version = "4.0.1_for_spark_3")

    testImplementation(group = "junit", name = "junit", version = "null")
    testImplementation(group = "org.scalatest", name = "scalatest_$scalaVersion", version = "3.1.2")
    testImplementation(group = "org.scalatestplus", name = "junit-4-12_$scalaVersion", version = "3.1.2.0")

    testRuntimeOnly(group = "org.scala-lang.modules", name = "scala-xml_$scalaVersion", version = "1.2.0")
}
