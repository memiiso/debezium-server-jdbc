[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
![contributions welcome](https://img.shields.io/badge/contributions-welcome-brightgreen.svg?style=flat)
![Java CI](https://github.com/memiiso/debezium-server-jdbc/workflows/Java%20CI%20with%20Maven/badge.svg?branch=master)

# Debezium Jdbc Consumers

This project adds JDBC database consumer
to [Debezium Server](https://debezium.io/documentation/reference/operations/debezium-server.html). It could be used to
replicate database CDC changes to JDBC database table in realtime, without requiring Spark, Kafka or Streaming platform.

More detail available in [documentation page](docs/DOCS.md)
Also, check [caveats](docs/CAVEATS.md) for better understanding the current limitation and proper workaround

# Install from source

- Requirements:
    - JDK 11
    - Maven
- Clone from repo: `git clone https://github.com/memiiso/debezium-server-jdbc.git`
- From the root of the project:
    - Build and package debezium server: `mvn -Passembly -Dmaven.test.skip package`
    - After building, unzip your server
      distribution: `unzip debezium-server-jdbc-dist/target/debezium-server-jdbc-dist*.zip -d appdist`
    - cd into unzipped folder: `cd appdist`
    - Create `application.properties` file and config it: `nano conf/application.properties`, you can check the example
      configuration
      in [application.properties.example](debezium-server-jdbc-sink/src/main/resources/conf/application.properties.example)
    - Run the server using provided script: `bash run.sh`

# Contributing

The Memiiso community welcomes anyone that wants to help out in any way, whether that includes reporting problems,
helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features.
See [contributing document](CONTRIBUTING.md) for details.

### Contributors

<a href="https://github.com/memiiso/debezium-server-jdbc/graphs/contributors">
  <img src="https://contributors-img.web.app/image?repo=memiiso/debezium-server-jdbc" />
</a>