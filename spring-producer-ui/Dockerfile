FROM eclipse-temurin:21-jre-alpine
RUN mkdir -p /data
WORKDIR /app
COPY target/spring-producer-ui-1.0.0-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-Xmx128m", "-Xms32m", "-XX:+UseContainerSupport", "-jar", "app.jar"]
