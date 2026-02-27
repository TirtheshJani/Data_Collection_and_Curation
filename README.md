# Real-time Employee Salary Processing Pipeline

This project implements a real-time data streaming pipeline using Apache Spark Structured Streaming, Kafka, and MySQL. It processes employee data, categorizes it based on salary, and stores the results in both MySQL and Kafka for downstream consumption.

## Architecture

1.  **Data Source**: A data producer generates random employee data (Id, Name, Department, Salary) and pushes it to a Kafka topic (`Employeefinal`).
2.  **Processing**: A Spark Structured Streaming application reads from the Kafka topic.
    *   It parses the JSON data.
    *   It filters employees into two categories:
        *   **High Salary**: Salary >= 20,000
        *   **Low Salary**: Salary < 20,000
3.  **Sinks**:
    *   **MySQL**: Data is written to `high_salary` and `low_salary` tables.
    *   **Kafka**: Data is written back to `high_salary` and `low_salary` topics.

## Prerequisites

*   Docker and Docker Compose
*   Java 1.8+ (Tested with Java 17/21)
*   Maven

## Project Structure

```
.
├── docker-compose.yml          # Infrastructure setup (Kafka, Zookeeper, MySQL)
├── init.sql                    # MySQL initialization script
├── pom.xml                     # Maven build configuration
├── src
│   ├── main
│   │   ├── resources
│   │   │   └── application.properties # Configuration file
│   │   └── scala
│   │       └── com
│   │           └── capstone
│   │               ├── EmployeeDataProducer.scala # Generates test data
│   │               └── SalaryProcessor.scala      # Main processing logic
│   └── test
│       └── scala
│           └── com
│               └── capstone
│                   └── SalaryProcessorTest.scala  # Unit tests
```

## Setup & Running

### 1. Start Infrastructure

Start Kafka, Zookeeper, and MySQL using Docker Compose:

```bash
docker-compose up -d
```

### 2. Build the Project

Compile the Scala code and run tests:

```bash
mvn clean package
```

### 3. Run the Data Producer

This will start generating random employee data and sending it to the `Employeefinal` Kafka topic.

```bash
mvn scala:run -DmainClass=com.capstone.EmployeeDataProducer
```

### 4. Run the Data Processor

This will start the Spark Streaming application.

```bash
mvn scala:run -DmainClass=com.capstone.SalaryProcessor
```

### 5. Verify Data

You can check the MySQL database to see the populated tables:

```bash
docker exec -it <mysql_container_id> mysql -u root -ppassword EmployeeTest
```

```sql
SELECT * FROM high_salary LIMIT 10;
SELECT * FROM low_salary LIMIT 10;
```

## Configuration

Configuration settings (Kafka brokers, topics, MySQL credentials) are located in `src/main/resources/application.properties`.
