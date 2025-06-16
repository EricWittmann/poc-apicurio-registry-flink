#!/bin/bash

mvn clean package
java --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.util=ALL-UNNAMED --add-opens java.base/java.lang.reflect=ALL-UNNAMED -jar target/poc-apicurio-registry-flink-1.0-SNAPSHOT.jar
