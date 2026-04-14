package com.capstone

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.internal.Logging
import java.util.Properties

/**
 * Generates synthetic employee data and publishes it to a Kafka topic.
 *
 * This producer uses Spark's built-in rate source to create a continuous stream
 * of randomized employee records (Id, Name, Department, Salary, timestamp) and
 * writes them as JSON to a configurable Kafka topic for downstream processing.
 *
 * Configuration is loaded from `application.properties` on the classpath.
 *
 * @see [[SalaryProcessor]] for the downstream consumer that categorizes records by salary.
 */
object EmployeeDataProducer extends Logging {

  /** Loads configuration properties from the classpath. */
  private def loadConfig(): Properties = {
    val properties = new Properties()
    val configStream = getClass.getClassLoader.getResourceAsStream("application.properties")
    if (configStream == null) {
      throw new RuntimeException("Configuration file 'application.properties' not found in classpath")
    }
    try {
      properties.load(configStream)
    } finally {
      configStream.close()
    }
    properties
  }

  def main(args: Array[String]): Unit = {
    val properties = loadConfig()

    val kafkaBootstrapServers: String = properties.getProperty("kafka.bootstrap.servers")
    val topic: String = properties.getProperty("kafka.topic.input")
    val checkpointBaseDir: String = properties.getProperty("checkpoint.base.dir")

    logInfo(s"Starting EmployeeDataProducer — target topic: $topic, brokers: $kafkaBootstrapServers")

    val spark: SparkSession = SparkSession.builder
      .appName("EmployeeDataProducer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val departments: Seq[String] = Seq("Engineering", "HR", "Sales", "Marketing", "Finance")

    val rateStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()

    val employeeData = rateStream
      .withColumn("Id", expr("uuid()"))
      .withColumn("Name", expr("concat('Employee_', cast(rand()*1000 as int))"))
      .withColumn("Department", expr(
        s"elt(cast(rand()*${departments.size} as int) + 1, ${departments.map(d => s"'$d'").mkString(",")})"
      ))
      .withColumn("Salary", (rand() * 40000 + 10000).cast("int"))
      .withColumn("timestamp", current_timestamp())
      .select(to_json(struct($"Id", $"Name", $"Department", $"Salary", $"timestamp")).alias("value"))

    logInfo("Streaming query started — writing employee records to Kafka")

    val query = employeeData.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", topic)
      .option("checkpointLocation", s"$checkpointBaseDir/producer")
      .start()

    query.awaitTermination()
  }
}
