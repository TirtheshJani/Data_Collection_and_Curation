package com.capstone

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import java.util.Properties
import java.io.FileInputStream

object SalaryProcessor {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("EmployeeSalaryProcessor")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    // Load configuration
    val properties = new Properties()
    val configStream = SalaryProcessor.getClass.getClassLoader.getResourceAsStream("application.properties")
    if (configStream != null) {
      properties.load(configStream)
    } else {
      throw new RuntimeException("Configuration file 'application.properties' not found in classpath")
    }

    val kafkaBootstrapServers = properties.getProperty("kafka.bootstrap.servers")
    val inputTopic = properties.getProperty("kafka.topic.input")
    val highSalaryTopic = properties.getProperty("kafka.topic.high_salary")
    val lowSalaryTopic = properties.getProperty("kafka.topic.low_salary")

    val jdbcUrl = properties.getProperty("mysql.url")
    val connectionProperties = new Properties()
    connectionProperties.put("user", properties.getProperty("mysql.user"))
    connectionProperties.put("password", properties.getProperty("mysql.password"))
    connectionProperties.put("driver", properties.getProperty("mysql.driver"))

    val checkpointBaseDir = properties.getProperty("checkpoint.base.dir")

    val employeeSchema = StructType(Seq(
      StructField("Id", StringType),
      StructField("Name", StringType),
      StructField("Department", StringType),
      StructField("Salary", IntegerType),
      StructField("timestamp", TimestampType)
    ))

    // Set up Kafka source to read data from the "Employee" topic
    val employeeUserDF = spark.readStream
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

    // Process the data and categorize it
    val highSalary = employeeUserDF.filter($"Salary" >= 20000)
    val lowSalary = employeeUserDF.filter($"Salary" < 20000)

    // Serialize the data for writing to Kafka
    val highSalaryJson = highSalary.select(to_json(struct(col("*"))).cast("string").as("value"))
    val lowSalaryJson = lowSalary.select(to_json(struct(col("*"))).cast("string").as("value"))

    // Write to MySQL
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

    // Write back to Kafka
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

    // Await termination of all streams
    spark.streams.awaitAnyTermination()
  }
}
