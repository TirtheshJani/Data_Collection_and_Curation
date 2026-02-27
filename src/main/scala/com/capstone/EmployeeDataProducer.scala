package com.capstone

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.util.Properties

object EmployeeDataProducer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("EmployeeDataProducer")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load configuration
    val properties = new Properties()
    val configStream = EmployeeDataProducer.getClass.getClassLoader.getResourceAsStream("application.properties")
    if (configStream != null) {
      properties.load(configStream)
    } else {
      throw new RuntimeException("Configuration file 'application.properties' not found in classpath")
    }

    val kafkaBootstrapServers = properties.getProperty("kafka.bootstrap.servers")
    val topic = properties.getProperty("kafka.topic.input")

    // Generate random data
    val departments = Seq("Engineering", "HR", "Sales", "Marketing", "Finance")

    val rateStream = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 1)
      .load()

    val employeeData = rateStream
      .withColumn("Id", expr("uuid()"))
      .withColumn("Name", expr("concat('Employee_', cast(rand()*1000 as int))"))
      .withColumn("Department", expr(s"elt(cast(rand()*${departments.size} as int) + 1, ${departments.map(d => s"'$d'").mkString(",")})"))
      .withColumn("Salary", (rand() * 40000 + 10000).cast("int")) // Salary between 10,000 and 50,000
      .withColumn("timestamp", current_timestamp())
      .select(to_json(struct($"Id", $"Name", $"Department", $"Salary", $"timestamp")).alias("value"))

    val query = employeeData.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", topic)
      .option("checkpointLocation", s"/tmp/checkpoints/producer")
      .start()

    query.awaitTermination()
  }
}
