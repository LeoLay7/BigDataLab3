FROM maven:3.9.8-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml ./
COPY src ./src
RUN mvn -q -DskipTests package

FROM flink:1.18.1-java17
WORKDIR /opt/flink

COPY --from=builder /build/target/flink-module-0.0.1-SNAPSHOT.jar /opt/flink/usrlib/flink-module.jar
COPY docker/flink-entrypoint.sh /opt/flink/flink-entrypoint.sh
RUN chmod +x /opt/flink/flink-entrypoint.sh

CMD ["bash", "/opt/flink/flink-entrypoint.sh"]