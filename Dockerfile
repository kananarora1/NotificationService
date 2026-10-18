# Use Java 21.
FROM maven:3.9-eclipse-temurin-21@sha256:99e61abcff91a9b1333463bd8451fb18495d6eba9250ac66a338b518f8278320 AS build

WORKDIR /workspace

COPY pom.xml ./

COPY src/ src/
# Tests run in CI.
RUN --mount=type=cache,target=/root/.m2 mvn -B -Dmaven.test.skip=true package \
    && cp target/notification-service-*.jar /workspace/app.jar

# Run as non-root.
FROM eclipse-temurin:21-jre-jammy@sha256:affcab5356e1270ac35d2361ee83135af2f03e8d580eca78a6d5d6fdbf048db0 AS runtime

RUN groupadd --system --gid 10001 spring \
    && useradd --system --uid 10001 --gid spring --create-home spring

WORKDIR /app
COPY --from=build --chown=spring:spring /workspace/app.jar app.jar

USER spring:spring

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]
