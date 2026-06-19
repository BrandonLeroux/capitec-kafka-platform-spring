FROM eclipse-temurin:21-jre-alpine
RUN mkdir -p /data
WORKDIR /app
COPY target/spring-payment-processor-1.0.0-SNAPSHOT.jar app.jar

ENTRYPOINT ["java", "-Xmx128m", "-Xms32m", "-XX:+UseContainerSupport", "-jar", "app.jar"]
