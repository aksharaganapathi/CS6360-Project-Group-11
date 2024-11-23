# Use an official Maven image to build the application
FROM maven:3.8.4-openjdk-17 AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Use an official OpenJDK image to run the application
FROM openjdk:17-slim
WORKDIR /app
COPY --from=build /app/target/epoxy-implementation-1.0-SNAPSHOT.jar ./epoxy-implementation.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "epoxy-implementation.jar"]