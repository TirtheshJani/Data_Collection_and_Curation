package com.capstone

import org.apache.spark.sql.SparkSession
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

class SalaryProcessorTest extends AnyFunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("SalaryProcessorTest")
      .master("local[*]")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("High salary filter should only include salaries >= 20000") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      ("1", "Alice", "IT", 25000),
      ("2", "Bob", "HR", 15000),
      ("3", "Charlie", "Sales", 20000)
    ).toDF("Id", "Name", "Department", "Salary")

    val highSalary = data.filter($"Salary" >= 20000)

    val results = highSalary.collect()
    assert(results.length == 2)
    assert(results.exists(r => r.getString(1) == "Alice"))
    assert(results.exists(r => r.getString(1) == "Charlie"))
  }

  test("Low salary filter should only include salaries < 20000") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      ("1", "Alice", "IT", 25000),
      ("2", "Bob", "HR", 15000),
      ("3", "Charlie", "Sales", 20000)
    ).toDF("Id", "Name", "Department", "Salary")

    val lowSalary = data.filter($"Salary" < 20000)

    val results = lowSalary.collect()
    assert(results.length == 1)
    assert(results.exists(r => r.getString(1) == "Bob"))
  }
}
