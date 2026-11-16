# The Topic Scope server as one image: MCP over stdin/stdout (the default), MCP over HTTP, or the status board.
#
#   docker build -t topic-scope-mcp:0.1.1 .
#
# How to run each mode is in docs/usage.md. Build context is the repository root.

# ---------- 1. the status board's page ----------
FROM node:24-alpine AS page
WORKDIR /board
COPY board/package.json board/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY board/ ./
RUN npm run build

# ---------- 2. the server jar ----------
FROM eclipse-temurin:21-jdk AS build
# Without unzip the Maven wrapper downloads the .tar.gz instead of the .zip, and the pinned SHA-256 is the .zip's.
RUN apt-get update && apt-get install -y --no-install-recommends unzip && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY mvnw pom.xml ./
COPY .mvn .mvn
COPY simulator/pom.xml simulator/pom.xml
COPY simulator/src/main simulator/src/main
COPY server/pom.xml server/pom.xml
COPY server/src/main server/src/main
# The tests are run by ./mvnw verify on a developer's machine; they need Docker themselves. Here only the jar is built.
# The simulator is built along because the server's tests depend on it and Maven resolves that even when tests are skipped.
RUN --mount=type=cache,target=/root/.m2 ./mvnw -B -q -pl server -am -Dmaven.test.skip=true package \
    && cp server/target/topic-scope-server-*.jar /topic-scope-server.jar

# ---------- 3. what runs ----------
FROM eclipse-temurin:21-jre
RUN useradd --system --create-home --home-dir /home/topicscope topicscope \
    && mkdir /data && chown topicscope /data
USER topicscope
WORKDIR /app
COPY --from=build /topic-scope-server.jar /app/topic-scope-server.jar
COPY --from=page /board/dist /app/board/dist

# The audit log goes to /data; mount a volume there to keep it.
# Inside a container "this machine only" would mean "this container only", so board mode and HTTP mode listen on
# all of the container's interfaces. Who can reach them is decided where the port is published:
# publish on 127.0.0.1 (-p 127.0.0.1:8090:8090) unless you mean to share it.
ENV TOPIC_SCOPE_AUDIT_FILE=/data/audit.jsonl \
    TOPIC_SCOPE_BOARD_FILES=file:/app/board/dist/ \
    TOPIC_SCOPE_BOARD_ADDRESS=0.0.0.0 \
    TOPIC_SCOPE_HTTP_ADDRESS=0.0.0.0
VOLUME /data
EXPOSE 8090 8091
ENTRYPOINT ["java", "-jar", "/app/topic-scope-server.jar"]
