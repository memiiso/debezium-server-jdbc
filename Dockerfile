FROM eclipse-temurin:11-jdk as builder
RUN apt-get -qq update && apt-get -qq install maven unzip
COPY . /app
WORKDIR /app
RUN mvn clean package -Passembly -Dmaven.test.skip --quiet
RUN unzip /app/debezium-server-jdbc-dist/target/debezium-server-jdbc-dist*.zip -d appdist

FROM eclipse-temurin:11-jre
COPY --from=builder /app/appdist/debezium-server-jdbc/ /app/

WORKDIR /app
EXPOSE 8080 8083
VOLUME ["/app/conf", "/app/data"] 

ENTRYPOINT ["/app/run.sh"]