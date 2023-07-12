# Final-Project-Data-Collection-and-Curation-
This Project demonstrates a Spark Streaming application developed in Scala that makes use of Apache Spark's Structured Streaming API. It shows how to read data from Kafka, process the data, and write the results to a MySQL database and Kafka topics.
The following is a summary of what the code does:
1. It loads the necessary Spark and streaming libraries.
2. It creates a SparkSession, which serves as the starting point for dealing with structured data in Spark.
3. It specifies the schema for employee data.
4. It creates a Kafka source to read information from the "Employeefinal" topic.
5. It processes the data by separating employees with high incomes (>= 20000) from those with low salaries ( 20000).
6. It serialises the filtered data into JSON format so that it may be written to Kafka.
7. It sets up the MySQL database connection properties.
8. It defines a streaming query to publish the high-salary data to the MySQL database's "high_salary" table.
9. It defines another streaming query to publish the low-salary data to the MySQL database's "low_salary" table.
10. It launches the streaming queries to process and write the data in real time.
11.	It is waiting for the streaming queries to finish.
12.	The high-salary data is written to the "high_salary" Kafka topic.
13.	The low-salary data is written to the "low_salary" Kafka topic.

In short, this code accepts streaming data from Kafka, processes it by categorising employees based on their wage, publishes the processed data to a MySQL database, and also writes the categorised data to separate Kafka topics.
