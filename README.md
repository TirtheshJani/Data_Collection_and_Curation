# Real-Time Employee Salary Processing Pipeline

[![CI](https://github.com/TirtheshJani/Data_Collection_and_Curation/actions/workflows/ci.yml/badge.svg)](https://github.com/TirtheshJani/Data_Collection_and_Curation/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
![Scala](https://img.shields.io/badge/Scala-2.12-red?logo=scala)
![Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-E25A1C?logo=apachespark&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4.0-231F20?logo=apachekafka&logoColor=white)

A production-style data streaming application that ingests employee records in real time, classifies them by salary, and sinks the results to both **MySQL** and **Kafka** for downstream analytics.

Built with **Apache Spark Structured Streaming**, **Apache Kafka**, and **MySQL** — fully containerized with **Docker Compose**.

---

## Architecture

```
┌──────────────────┐       ┌───────────────┐       ┌──────────────────────────────┐
│  Data Producer   │──────▶│  Kafka Topic   │──────▶│  Spark Structured Streaming  │
│ (rate source +   │       │ Employeefinal  │       │     SalaryProcessor          │
│  random records) │       └───────────────┘       └──────────┬───────────────────┘
└──────────────────┘                                          │
                                                   ┌──────────┴──────────┐
                                                   ▼                     ▼
                                          Salary >= 20,000       Salary < 20,000
                                                   │                     │
                                          ┌────────┴────────┐  ┌────────┴────────┐
                                          ▼                 ▼  ▼                 ▼
                                     Kafka Topic      MySQL    Kafka Topic    MySQL
                                     high_salary    high_salary low_salary  low_salary
```

**Pipeline stages:**

1. **Ingest** — `EmployeeDataProducer` generates randomized employee records (Id, Name, Department, Salary) at a configurable rate and publishes JSON to a Kafka topic.
2. **Parse** — `SalaryProcessor` subscribes to the topic and deserializes each JSON payload against a predefined schema.
3. **Classify** — Records are split into *high salary* (>= 20,000) and *low salary* (< 20,000) streams.
4. **Sink** — Each category is written to a dedicated MySQL table *and* a dedicated Kafka topic for downstream consumers.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Scala 2.12 |
| Stream Processing | Apache Spark 3.5.0 (Structured Streaming) |
| Message Broker | Apache Kafka (Confluent 7.4.0) |
| Database | MySQL 8.0 |
| Build Tool | Apache Maven |
| Testing | ScalaTest 3.2 |
| Containerization | Docker & Docker Compose |
| CI | GitHub Actions |

---

## Project Structure

```
.
├── .github/workflows/ci.yml        # CI pipeline (build + test)
├── docker-compose.yml               # Kafka, Zookeeper, MySQL infrastructure
├── Dockerfile                       # Multi-stage application image
├── init.sql                         # Database schema with indexes
├── pom.xml                          # Maven build configuration
└── src
    ├── main
    │   ├── resources
    │   │   └── application.properties   # Externalized configuration
    │   └── scala/com/capstone
    │       ├── EmployeeDataProducer.scala  # Synthetic data generator
    │       └── SalaryProcessor.scala       # Stream processing & classification
    └── test
        └── scala/com/capstone
            └── SalaryProcessorTest.scala    # Unit & edge-case tests
```

---

## Getting Started

### Prerequisites

- **Docker** and **Docker Compose**
- **Java 17+** (tested with JDK 17 and 21)
- **Maven 3+**

### 1. Start Infrastructure

Spin up Kafka, Zookeeper, and MySQL (with automatic schema creation):

```bash
docker-compose up -d
```

Wait for all services to be healthy:

```bash
docker-compose ps
```

### 2. Build the Project

```bash
mvn clean package
```

### 3. Run the Data Producer

Start generating synthetic employee records:

```bash
mvn scala:run -DmainClass=com.capstone.EmployeeDataProducer
```

### 4. Run the Salary Processor

In a separate terminal, start the streaming application:

```bash
mvn scala:run -DmainClass=com.capstone.SalaryProcessor
```

### 5. Verify Results

Connect to MySQL and query the categorized data:

```bash
docker exec -it mysql mysql -u root -ppassword EmployeeTest
```

```sql
SELECT * FROM high_salary LIMIT 10;
SELECT * FROM low_salary  LIMIT 10;
SELECT COUNT(*) AS total_high FROM high_salary;
SELECT COUNT(*) AS total_low  FROM low_salary;
```

---

## Configuration

All settings are externalized in [`src/main/resources/application.properties`](src/main/resources/application.properties):

| Property | Description | Default |
|---|---|---|
| `kafka.bootstrap.servers` | Kafka broker addresses | `localhost:9092` |
| `kafka.topic.input` | Input topic for raw employee data | `Employeefinal` |
| `kafka.topic.high_salary` | Output topic for high salary records | `high_salary` |
| `kafka.topic.low_salary` | Output topic for low salary records | `low_salary` |
| `mysql.url` | JDBC connection URL | `jdbc:mysql://localhost:3306/EmployeeTest` |
| `mysql.user` | Database user | `root` |
| `mysql.password` | Database password | `${MYSQL_PASSWORD}` |
| `checkpoint.base.dir` | Spark checkpoint directory | `/tmp/checkpoints` |

---

## Testing

Run the full test suite:

```bash
mvn test
```

The test suite covers:
- Schema validation
- High/low salary filter logic
- Boundary conditions (exact threshold value)
- Mutual exclusivity and completeness of classification
- Empty dataset handling
- JSON serialization round-trip

---

## Teardown

Stop and remove all containers and volumes:

```bash
docker-compose down -v
```

---

## License

This project is licensed under the MIT License — see the [LICENSE](LICENSE) file for details.
