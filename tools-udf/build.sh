#!/usr/bin/env bash
# Build the scheduling-tools UDF JAR inside a Maven/JDK Docker container so no Java
# toolchain is required on the host. Produces target/scheduling-tools.jar, which is
# committed to the repo and uploaded to Confluent Cloud by Terraform
# (confluent_flink_artifact). Demo users do NOT need to run this — only run it
# when the Java tool sources change.
set -euo pipefail

cd "$(dirname "$0")"

docker run --rm \
  -v "$PWD":/app \
  -v "$HOME/.m2":/root/.m2 \
  -w /app \
  maven:3.9-eclipse-temurin-17 \
  mvn -q -DskipTests clean package

echo "Built: $(pwd)/target/scheduling-tools.jar"
