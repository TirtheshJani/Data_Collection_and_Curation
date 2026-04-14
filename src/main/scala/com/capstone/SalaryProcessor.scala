package com.capstone

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.internal.Logging
import java.util.Properties

/**
 * Real-time salary classification processor.
 *
 * Reads employee JSON records from a Kafka topic, classifies each record as
 * "high salary" (>= 20,000) or "low salary" (< 20,000), and writes the
 * categorized results to both MySQL tables and dedicated Kafka topics.
 *
 * ==Pipeline==
 *  1. '''Ingest''' — subscribe to the input Kafka topic.
 *  2. '''Parse''' — deserialize JSON payloads using a predefined schema.
 *  3. '''Classify''' — split the stream by salary threshold.
 *  4. '''Sink''' — write each category to MySQL (via JDBC) and to separate Kafka topics.
 *
 * Configuration is loaded from `application.properties` on the classpath.
 *
 * @see [[EmployeeDataProducer]] for the upstream data generator.
 */
object SalaryProcessor extends Logging {

  /** Salary threshold used to classify employees. */
  val SalaryThreshold: Int = 20000

  /** Schema for the inbound employee JSON records. */
  val employeeSchema: StructType = StructType(Seq(
    StructField("Id", StringType),
    StructField("Name", StringType),
    StructField("Department", StringType),
    StructField("Salary", IntegerType),
    StructField("timestamp", TimestampType)
  ))

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
    val inputTopic: String = properties.getProperty("kafka.topic.input")
    val highSalaryTopic: String = properties.getProperty("kafka.topic.high_salary")
    val lowSalaryTopic: String = properties.getProperty("kafka.topic.low_salary")

    val jdbcUrl: String = properties.getProperty("mysql.url")
    val connectionProperties = new Properties()
    connectionProperties.put("user", properties.getProperty("mysql.user"))
    connectionProperties.put("password", properties.getProperty("mysql.password"))
    connectionProperties.put("driver", properties.getProperty("mysql.driver"))

    val checkpointBaseDir: String = properties.getProperty("checkpoint.base.dir")

    logInfo(s"Starting SalaryProcessor — reading from topic: $inputTopic, threshold: $SalaryThreshold")

    val spark: SparkSession = SparkSession.builder
      .appName("EmployeeSalaryProcessor")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Ingest: subscribe to Kafka and parse JSON payloads
    val employeeDF: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", inputTopic)
      .option("startingOffsets", "latest")
      .load()
      .selectExpr("CAST(value AS STRING) as json")
      .select(from_json($"json", employeeSchema).as("employee"))
      .select(
        $"employee.Id".alias("Id"),
        $"employee.Name".alias("Name"),
        $"employee.Department".alias("Department"),
        $"employee.Salary".cast("integer").alias("Salary"),
        $"employee.timestamp".alias("timestamp")
      )

    // Classify by salary threshold
    val highSalary: DataFrame = employeeDF.filter($"Salary" >= SalaryThreshold)
    val lowSalary: DataFrame = employeeDF.filter($"Salary" < SalaryThreshold)

    val highSalaryJson: DataFrame = highSalary.select(to_json(struct(col("*"))).cast("string").as("value"))
    val lowSalaryJson: DataFrame = lowSalary.select(to_json(struct(col("*"))).cast("string").as("value"))

    // Sink: write to MySQL via JDBC
    val highSalaryMySQLQuery = highSalary.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .mode(SaveMode.Append)
          .jdbc(jdbcUrl, "high_salary", connectionProperties)
      }
      .option("checkpointLocation", s"$checkpointBaseDir/mysql/high_salary")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    val lowSalaryMySQLQuery = lowSalary.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .mode(SaveMode.Append)
          .jdbc(jdbcUrl, "low_salary", connectionProperties)
      }
      .option("checkpointLocation", s"$checkpointBaseDir/mysql/low_salary")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    // Sink: write categorized records back to Kafka
    val highSalaryKafkaQuery = highSalaryJson.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", highSalaryTopic)
      .option("checkpointLocation", s"$checkpointBaseDir/kafka/high_salary")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    val lowSalaryKafkaQuery = lowSalaryJson.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", lowSalaryTopic)
      .option("checkpointLocation", s"$checkpointBaseDir/kafka/low_salary")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    logInfo("All streaming queries started — awaiting termination")

    spark.streams.awaitAnyTermination()
  }
}
