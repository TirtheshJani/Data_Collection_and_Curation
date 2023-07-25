# Project-Data-Collection-and-Curation-
In this code, I developed a real-time data streaming pipeline using Apache Spark, Kafka, and JDBC connection to a MySQL database. The purpose of the pipeline is to process real-time employee data and categorize it based on the salary of employees. Here is what I did:

1. Spark set up: I began by creating a SparkSession, the entry point to any Spark functionality. I named it "EmployeeSalaryProcessor" and set it to run locally.

2. Defining the schema: I defined the schema for the employee data, which includes fields like Id, Name, Department, Salary, and a timestamp.

3. Establishing Kafka source: I set up a Kafka source to read data from a Kafka topic named "Employeefinal". The Kafka server is running locally on port 9092.

4. Data extraction and transformation: After reading the data, I extracted and transformed it from Kafka's format to a DataFrame. I did this by casting the data to string and parsing it as JSON. I then selected the necessary fields, converting the Salary to an integer and adding a timestamp to each row.

5. Data processing: I split the DataFrame into two categories: high salary (salary >= 20000) and low salary (salary < 20000).

6. Serialization: I converted both DataFrames back to JSON format in preparation for writing them back to Kafka.

7. JDBC connection setup: I set up a JDBC connection to a MySQL database with the necessary credentials and driver information.

8. Writing to MySQL: Using the writeStream functionality, I wrote the high salary and low salary data to two different tables ("high_salary" and "low_salary") in the MySQL database in batch mode. The data is appended to these tables every 10 seconds.

9. Writing back to Kafka: I also wrote the high and low salary data back to two separate Kafka topics, "high_salary" and "low_salary", using Spark's writeStream function. The data in Kafka is updated every 10 seconds as well.


So, in a nutshell, this program reads real-time employee data from a Kafka topic, processes it in Spark to categorize employees into high and low salary groups, writes the results to a MySQL database and back to separate Kafka topics for further downstream processing.
