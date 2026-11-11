# Build stage: compile and package with the Maven wrapper.
FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY .mvn .mvn
COPY mvnw pom.xml ./
RUN ./mvnw -B -q dependency:go-offline
COPY src src
RUN ./mvnw -B -q package -DskipTests

# Runtime stage: just a JRE and the jar.
FROM eclipse-temurin:21-jre
WORKDIR /app
RUN useradd --system --uid 1001 scheduler
COPY --from=build /src/target/job-scheduler-*.jar app.jar
USER scheduler
EXPOSE 8080
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "app.jar"]
