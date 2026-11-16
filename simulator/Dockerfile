# Build context is the repository root (see compose.yaml).

FROM eclipse-temurin:21-jdk AS build
# Without unzip the Maven wrapper downloads the .tar.gz instead of the .zip, and the pinned SHA-256 is the .zip's.
RUN apt-get update && apt-get install -y --no-install-recommends unzip && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY simulator/pom.xml simulator/pom.xml
COPY simulator/src/main simulator/src/main
# Tests are run by ./mvnw verify on the host; the image only needs the jar. Maven's downloads are cached between builds.
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q -pl simulator -DskipTests package \
    && cp simulator/target/topic-scope-simulator-*-all.jar /simulator.jar

FROM eclipse-temurin:21-jre
RUN useradd --system --no-create-home simulator
USER simulator
COPY --from=build /simulator.jar /app/simulator.jar
ENTRYPOINT ["java", "-jar", "/app/simulator.jar"]
