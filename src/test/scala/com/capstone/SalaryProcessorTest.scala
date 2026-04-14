package com.capstone

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
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

  // ---------------------------------------------------------------------------
  // Schema validation
  // ---------------------------------------------------------------------------

  test("Employee schema should have the correct fields and types") {
    val schema = SalaryProcessor.employeeSchema
    assert(schema.fields.length == 5)
    assert(schema("Id").dataType == StringType)
    assert(schema("Name").dataType == StringType)
    assert(schema("Department").dataType == StringType)
    assert(schema("Salary").dataType == IntegerType)
    assert(schema("timestamp").dataType == TimestampType)
  }

  // ---------------------------------------------------------------------------
  // Salary threshold constant
  // ---------------------------------------------------------------------------

  test("Salary threshold should be 20000") {
    assert(SalaryProcessor.SalaryThreshold == 20000)
  }

  // ---------------------------------------------------------------------------
  // High salary filter
  // ---------------------------------------------------------------------------

  test("High salary filter should include salaries >= 20000") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      ("1", "Alice", "IT", 25000),
      ("2", "Bob", "HR", 15000),
      ("3", "Charlie", "Sales", 20000)
    ).toDF("Id", "Name", "Department", "Salary")

    val highSalary = data.filter($"Salary" >= SalaryProcessor.SalaryThreshold)
    val results = highSalary.collect()

    assert(results.length == 2)
    assert(results.exists(r => r.getAs[String]("Name") == "Alice"))
    assert(results.exists(r => r.getAs[String]("Name") == "Charlie"))
  }

  test("High salary filter should include the boundary value of 20000") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      ("1", "BoundaryEmployee", "IT", 20000)
    ).toDF("Id", "Name", "Department", "Salary")

    val highSalary = data.filter($"Salary" >= SalaryProcessor.SalaryThreshold)
    assert(highSalary.count() == 1)
  }

  test("High salary filter should exclude salary of 19999") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      ("1", "JustBelow", "IT", 19999)
    ).toDF("Id", "Name", "Department", "Salary")

    val highSalary = data.filter($"Salary" >= SalaryProcessor.SalaryThreshold)
    assert(highSalary.count() == 0)
  }

  // ---------------------------------------------------------------------------
  // Low salary filter
  // ---------------------------------------------------------------------------

  test("Low salary filter should include only salaries < 20000") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      ("1", "Alice", "IT", 25000),
      ("2", "Bob", "HR", 15000),
      ("3", "Charlie", "Sales", 20000)
    ).toDF("Id", "Name", "Department", "Salary")

    val lowSalary = data.filter($"Salary" < SalaryProcessor.SalaryThreshold)
    val results = lowSalary.collect()

    assert(results.length == 1)
    assert(results.exists(r => r.getAs[String]("Name") == "Bob"))
  }

  test("Low salary filter should exclude the boundary value of 20000") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      ("1", "BoundaryEmployee", "IT", 20000)
    ).toDF("Id", "Name", "Department", "Salary")

    val lowSalary = data.filter($"Salary" < SalaryProcessor.SalaryThreshold)
    assert(lowSalary.count() == 0)
  }

  // ---------------------------------------------------------------------------
  // Partitioning completeness
  // ---------------------------------------------------------------------------

  test("High and low salary filters should be mutually exclusive and exhaustive") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      ("1", "A", "IT", 10000),
      ("2", "B", "HR", 19999),
      ("3", "C", "Sales", 20000),
      ("4", "D", "Finance", 50000)
    ).toDF("Id", "Name", "Department", "Salary")

    val high = data.filter($"Salary" >= SalaryProcessor.SalaryThreshold)
    val low = data.filter($"Salary" < SalaryProcessor.SalaryThreshold)

    assert(high.count() + low.count() == data.count(),
      "Every record must be classified into exactly one category")
    assert(high.intersect(low).count() == 0,
      "No record should appear in both categories")
  }

  // ---------------------------------------------------------------------------
  // Edge cases
  // ---------------------------------------------------------------------------

  test("Filters should handle an empty dataset gracefully") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq.empty[(String, String, String, Int)]
      .toDF("Id", "Name", "Department", "Salary")

    val high = data.filter($"Salary" >= SalaryProcessor.SalaryThreshold)
    val low = data.filter($"Salary" < SalaryProcessor.SalaryThreshold)

    assert(high.count() == 0)
    assert(low.count() == 0)
  }

  test("Filter should handle zero salary") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      ("1", "Intern", "IT", 0)
    ).toDF("Id", "Name", "Department", "Salary")

    val low = data.filter($"Salary" < SalaryProcessor.SalaryThreshold)
    assert(low.count() == 1)
  }

  // ---------------------------------------------------------------------------
  // JSON serialization round-trip
  // ---------------------------------------------------------------------------

  test("JSON serialization should preserve all fields") {
    val sparkSession = spark
    import sparkSession.implicits._

    val data = Seq(
      ("uuid-1", "Alice", "Engineering", 30000)
    ).toDF("Id", "Name", "Department", "Salary")

    val jsonDF = data.select(to_json(struct(col("*"))).as("value"))
    val parsed = jsonDF.select(
      from_json($"value", StructType(Seq(
        StructField("Id", StringType),
        StructField("Name", StringType),
        StructField("Department", StringType),
        StructField("Salary", IntegerType)
      ))).as("e")
    ).select("e.*")

    val row = parsed.collect()(0)
    assert(row.getAs[String]("Id") == "uuid-1")
    assert(row.getAs[String]("Name") == "Alice")
    assert(row.getAs[String]("Department") == "Engineering")
    assert(row.getAs[Int]("Salary") == 30000)
  }
}
