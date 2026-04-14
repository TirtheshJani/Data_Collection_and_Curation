FROM maven:3.9-eclipse-temurin-17-alpine AS build
WORKDIR /app
COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests -B

FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/target/*.jar ./app.jar
COPY --from=build /app/target/dependency/ ./lib/
ENTRYPOINT ["java", "-cp", "app.jar:lib/*", "com.capstone.SalaryProcessor"]
