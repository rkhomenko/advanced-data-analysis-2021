plugins {
    scala
    `java-library`
}

repositories {
    mavenCentral()
}

val scalaVersion = "2.12"
val sparkVersion = "3.1.0"

dependencies {
    implementation(group = "org.scala-lang", name = "scala-library", version = "2.12.13")
    compileOnly(group = "org.apache.spark", name = "spark-core_$scalaVersion", version = sparkVersion)
    compileOnly(group = "org.apache.spark", name = "spark-sql_$scalaVersion", version = sparkVersion)
    compileOnly(group = "org.apache.spark", name = "spark-graphx_$scalaVersion", version = sparkVersion)

    testImplementation(group = "junit", name = "junit", version = "null")
    testImplementation(group = "org.scalatest", name = "scalatest_$scalaVersion", version = "3.1.2")
    testImplementation(group = "org.scalatestplus", name = "junit-4-12_$scalaVersion", version = "3.1.2.0")

    testRuntimeOnly(group = "org.scala-lang.modules", name = "scala-xml_$scalaVersion", version = "1.2.0")
}
