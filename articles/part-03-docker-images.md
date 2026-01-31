# Part 3: Building Production-Ready Spark Docker Images

**Mastering Spark on Kubernetes Series**

*Stop using the default Spark image. Learn how to build optimized, production-ready Docker images with cloud connectors, Delta Lake, monitoring, and all the dependencies your workloads need.*

---

## What You'll Build

By the end of this guide, you'll have:

- ✅ Understanding of all official Spark base image variants
- ✅ Multi-stage Dockerfile that's 40% smaller than naive builds
- ✅ Images with GCS, S3, and ADLS cloud connectors
- ✅ Delta Lake, Iceberg, and Hudi table format support
- ✅ Python virtual environments for PySpark jobs
- ✅ Prometheus monitoring built-in
- ✅ CI/CD pipeline for automated image builds
- ✅ Security scanning and best practices

**Reading Time:** 50 minutes  
**Hands-On Time:** 45 minutes  
**Prerequisites:** [Part 2: Environment Setup](./part-02-environment-setup.md)
**Visual Reference:** [View Diagrams & Tables (Interactive)](./diagrams-part3.html)

---

## Table of Contents

1. [Why Custom Images Matter](#why-custom-images)
2. [Official Spark Images: A Complete Breakdown](#official-images)
3. [Multi-Stage Builds: The Production Pattern](#multi-stage-builds)
4. [Cloud Storage Connectors](#cloud-connectors)
5. [Table Formats: Delta Lake, Iceberg, Hudi](#table-formats)
6. [Python Dependencies for PySpark](#python-dependencies)
7. [Monitoring: Prometheus JMX Exporter](#monitoring)
8. [Container Registries: GCR, ECR, ACR](#registries)
9. [Image Optimization Strategies](#optimization)
10. [Security Scanning & Best Practices](#security)
11. [CI/CD Pipeline for Image Builds](#cicd)
12. [Troubleshooting Common Issues](#troubleshooting)

---

## Why Custom Images Matter {#why-custom-images}

The default `apache/spark` image is designed for simplicity, not production. Here's what it's missing:

| What You Need | Default Image | Custom Image |
|--------------|---------------|--------------|
| Cloud storage (GCS/S3) | ❌ | ✅ |
| Delta Lake / Iceberg | ❌ | ✅ |
| Custom Python packages | ❌ | ✅ |
| Monitoring agents | ❌ | ✅ |
| Your application JAR | ❌ | ✅ |
| Security patches | ⚠️ Delayed | ✅ Immediate |

### The Cost of NOT Using Custom Images

Without custom images, you're forced to:
1. Download JARs at runtime (slow startup, network failures)
2. Install packages at runtime (non-deterministic, version drift)
3. Miss monitoring and observability
4. Accept whatever versions Spark ships with

**Production reality:** A custom image adds 5 minutes to your build pipeline but saves hours of debugging and runtime failures.

---

## Official Spark Images: A Complete Breakdown {#official-images}

Apache Spark provides several official images. Understanding them is crucial for choosing the right base.

### Available Tags (Spark 3.5.0)

```bash
# List available tags
docker search apache/spark
```

| Image Tag | Size | Python | Java | Use Case |
|-----------|------|--------|------|----------|
| `apache/spark:3.5.0` | ~590MB | ❌ | 17 | Scala/Java jobs only |
| `apache/spark:3.5.0-python3` | ~950MB | 3.10 | 17 | PySpark jobs |
| `apache/spark:3.5.0-scala2.12-java11-python3-ubuntu` | ~1.1GB | 3.10 | 11 | Full stack (explicit) |
| `apache/spark:3.5.0-scala2.12-java17-python3-ubuntu` | ~1.1GB | 3.10 | 17 | Full stack (Java 17) |
| `apache/spark:3.5.0-scala2.12-java17-r-ubuntu` | ~1.3GB | ❌ | 17 | SparkR jobs |

### Image Anatomy

Let's explore what's inside the official image:

```bash
# Inspect the image
docker run --rm -it apache/spark:3.5.0-python3 ls -la /opt/spark/

# Key directories:
# /opt/spark/bin       - spark-submit, pyspark, spark-shell
# /opt/spark/jars      - Core Spark JARs (~300 files)
# /opt/spark/conf      - Configuration templates
# /opt/spark/python    - PySpark source
# /opt/spark/examples  - Example applications
```

### What's NOT Included

```bash
# Missing cloud connectors
ls /opt/spark/jars/ | grep -E "(gcs|s3a|azure)" 
# (empty)

# Missing table formats
ls /opt/spark/jars/ | grep -E "(delta|iceberg|hudi)"
# (empty)
```

### Choosing Your Base Image

```
┌─────────────────────────────────────────────────────────┐
│              Choose Your Base Image                      │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  What language are your Spark jobs?                      │
│                                                          │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐ │
│  │   Scala /   │    │   PySpark   │    │   SparkR    │ │
│  │    Java     │    │             │    │             │ │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘ │
│         │                  │                   │        │
│         ▼                  ▼                   ▼        │
│  apache/spark:       apache/spark:      apache/spark:   │
│  3.5.0               3.5.0-python3     3.5.0-r-ubuntu  │
│  (590MB)             (950MB)           (1.3GB)          │
│                                                          │
└─────────────────────────────────────────────────────────┘
```

---

## Multi-Stage Builds: The Production Pattern {#multi-stage-builds}

Multi-stage builds are essential for production Docker images. They:
1. Keep build dependencies out of the final image
2. Reduce image size significantly
3. Improve security by minimizing attack surface

### The Pattern

```dockerfile
# Stage 1: Download/Build (throwaway)
FROM alpine AS builder
# Download JARs, compile code
# This layer is NOT in the final image

# Stage 2: Final Image (what runs)
FROM apache/spark:3.5.0-python3
# Copy only what you need from Stage 1
COPY --from=builder /jars/*.jar /opt/spark/jars/
```

### Complete Production Dockerfile

Here's a comprehensive Dockerfile for production Spark on GKE:

```dockerfile
# ============================================================
# Dockerfile.spark - Production Spark Image
# ============================================================
# Build: docker build -t spark-prod:3.5.0 -f Dockerfile.spark .
# ============================================================

# ============ STAGE 1: JAR Downloader ============
FROM alpine:3.19 AS jar-downloader

# Install curl for downloading
RUN apk add --no-cache curl

WORKDIR /jars

# ----- Google Cloud Storage Connector -----
# Required for reading/writing to GCS (gs://)
ARG GCS_CONNECTOR_VERSION=hadoop3-2.2.19
RUN curl -fSL -o gcs-connector.jar \
    "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/${GCS_CONNECTOR_VERSION}/gcs-connector-${GCS_CONNECTOR_VERSION}-shaded.jar"

# ----- AWS S3 Connector (Hadoop AWS) -----
# Required for reading/writing to S3 (s3a://)
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.367
RUN curl -fSL -o hadoop-aws.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fSL -o aws-java-sdk-bundle.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"

# ----- Azure ADLS Gen2 Connector -----
# Required for reading/writing to Azure (abfss://)
ARG HADOOP_AZURE_VERSION=3.3.4
RUN curl -fSL -o hadoop-azure.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/${HADOOP_AZURE_VERSION}/hadoop-azure-${HADOOP_AZURE_VERSION}.jar"

# ----- Delta Lake -----
ARG DELTA_VERSION=3.1.0
RUN curl -fSL -o delta-spark.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" && \
    curl -fSL -o delta-storage.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"

# ----- Apache Iceberg -----
ARG ICEBERG_VERSION=1.4.3
RUN curl -fSL -o iceberg-spark-runtime.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"

# ----- Apache Hudi -----
# ----- Apache Hudi -----
ARG HUDI_VERSION=0.14.1
# Note: Using spark3.4 bundle as 3.5 bundle is not yet available on Maven Central for 0.14.1
RUN curl -fSL -o hudi-spark-bundle.jar \
    "https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/${HUDI_VERSION}/hudi-spark3.4-bundle_2.12-${HUDI_VERSION}.jar"

# ----- Prometheus JMX Exporter -----
ARG JMX_EXPORTER_VERSION=0.20.0
RUN curl -fSL -o jmx_prometheus_javaagent.jar \
    "https://repo1.maven.org/maven2/io/prometheus/jmx/jmx_prometheus_javaagent/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar"

# Verify all JARs downloaded successfully
RUN ls -la /jars/ && \
    for jar in /jars/*.jar; do \
        echo "Verifying $jar..." && \
        unzip -t "$jar" > /dev/null || exit 1; \
    done

# ============ STAGE 2: Python Dependencies ============
FROM python:3.10-slim AS python-builder

WORKDIR /app

# Copy requirements and install
COPY requirements.txt .

# Create virtual environment and install packages
RUN python -m venv /opt/venv && \
    /opt/venv/bin/pip install --no-cache-dir --upgrade pip && \
    /opt/venv/bin/pip install --no-cache-dir -r requirements.txt

# ============ STAGE 3: Application Builder (Scala/Java) ============
# Uncomment if you have a Scala/Java application to build
# FROM sbt:eclipse-temurin-17_1.9.8 AS app-builder
# WORKDIR /app
# COPY build.sbt .
# COPY project/ project/
# RUN sbt update
# COPY src/ src/
# RUN sbt assembly
# Output: /app/target/scala-2.12/my-app-assembly.jar

# ============ STAGE 4: Final Image ============
FROM apache/spark:3.5.0-python3

# Metadata
LABEL maintainer="your-email@company.com"
LABEL description="Production Spark image with cloud connectors and monitoring"
LABEL spark.version="3.5.0"
LABEL delta.version="3.1.0"
LABEL iceberg.version="1.4.3"

USER root

# ----- Copy JARs from downloader stage -----
COPY --from=jar-downloader /jars/*.jar /opt/spark/jars/

# ----- Copy Python virtual environment -----
COPY --from=python-builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYSPARK_PYTHON=/opt/venv/bin/python
ENV PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python

# ----- Copy application JAR (if built) -----
# COPY --from=app-builder /app/target/scala-2.12/my-app-assembly.jar /opt/spark/jars/

# ----- Copy configuration files -----
COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY conf/prometheus-jmx-config.yaml /opt/spark/conf/prometheus-jmx-config.yaml
COPY conf/log4j2.properties /opt/spark/conf/log4j2.properties

# ----- Copy application code (PySpark) -----
COPY src/ /opt/spark/work-dir/src/

# ----- Fix permissions -----
RUN chmod -R 755 /opt/spark/jars/ && \
    chmod -R 755 /opt/spark/conf/ && \
    chmod -R 755 /opt/spark/work-dir/

# ----- Security: Remove unnecessary packages -----
RUN apt-get update && \
    apt-get remove -y --purge curl wget && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Switch back to spark user
USER spark
WORKDIR /opt/spark/work-dir
```

### Supporting Configuration Files

#### conf/spark-defaults.conf

```properties
# ============================================================
# Spark Defaults - Baked into Docker Image
# ============================================================

# ----- Adaptive Query Execution -----
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true

# ----- Serialization -----
spark.serializer=org.apache.spark.serializer.KryoSerializer
spark.kryoserializer.buffer.max=1024m

# ----- Delta Lake -----
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.databricks.delta.schema.autoMerge.enabled=true
spark.databricks.delta.merge.repartitionBeforeWrite.enabled=true

# ----- GCS Configuration -----
spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS
spark.hadoop.google.cloud.auth.service.account.enable=true

# ----- S3 Configuration -----
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain

# ----- Metrics -----
spark.ui.prometheus.enabled=true
spark.metrics.conf.*.sink.prometheusServlet.class=org.apache.spark.metrics.sink.PrometheusServlet

# ----- Memory -----
spark.memory.fraction=0.8
spark.memory.storageFraction=0.3
```

#### conf/prometheus-jmx-config.yaml

```yaml
# JMX Exporter configuration for Spark metrics
---
startDelaySeconds: 0
ssl: false
lowercaseOutputName: true
lowercaseOutputLabelNames: true

rules:
  # ----- Spark Driver Metrics -----
  - pattern: "metrics<name=(.+)\\.driver\\.(.*), type=(.+)><>Value"
    name: spark_driver_$2
    labels:
      app: "$1"
      type: "$3"
    type: GAUGE

  - pattern: "metrics<name=(.+)\\.driver\\.(.*), type=(.+)><>Count"
    name: spark_driver_$2_total
    labels:
      app: "$1"
      type: "$3"
    type: COUNTER

  # ----- Spark Executor Metrics -----
  - pattern: "metrics<name=(.+)\\.executor\\.(.*), type=(.+)><>Value"
    name: spark_executor_$2
    labels:
      app: "$1"
      type: "$3"
    type: GAUGE

  # ----- JVM Memory -----
  - pattern: "java.lang<type=Memory><HeapMemoryUsage>used"
    name: jvm_heap_memory_used_bytes
    type: GAUGE

  - pattern: "java.lang<type=Memory><NonHeapMemoryUsage>used"
    name: jvm_nonheap_memory_used_bytes
    type: GAUGE

  # ----- JVM GC -----
  - pattern: "java.lang<type=GarbageCollector, name=(.+)><>CollectionCount"
    name: jvm_gc_collection_count
    labels:
      gc: "$1"
    type: COUNTER

  - pattern: "java.lang<type=GarbageCollector, name=(.+)><>CollectionTime"
    name: jvm_gc_collection_time_ms
    labels:
      gc: "$1"
    type: COUNTER

  # ----- JVM Threads -----
  - pattern: "java.lang<type=Threading><>ThreadCount"
    name: jvm_thread_count
    type: GAUGE
```

#### conf/log4j2.properties

```properties
# Log4j2 configuration for Spark
status = error
name = SparkLog4j2Config

appender.console.type = Console
appender.console.name = console
appender.console.target = SYSTEM_ERR
appender.console.layout.type = PatternLayout
appender.console.layout.pattern = %d{yy/MM/dd HH:mm:ss} %p %c{1}: %m%n

# Root logger
rootLogger.level = WARN
rootLogger.appenderRef.console.ref = console

# Spark-specific logging
logger.spark.name = org.apache.spark
logger.spark.level = WARN

logger.jetty.name = org.sparkproject.jetty
logger.jetty.level = WARN

# Your application logging
logger.app.name = com.yourcompany
logger.app.level = INFO
```

#### requirements.txt

```text
# Python dependencies for PySpark jobs

# Data processing
pandas>=2.1.0
pyarrow>=14.0.1
numpy>=1.26.0

# Table formats
delta-spark==3.1.0
# Note: Iceberg and Hudi use JARs, not Python packages

# Cloud SDKs
google-cloud-storage>=2.13.0
google-cloud-bigquery>=3.13.0
boto3>=1.33.0

# Configuration
pyyaml>=6.0.1
python-dotenv>=1.0.0

# Observability
prometheus-client>=0.19.0
opentelemetry-api>=1.21.0

# Data validation
great-expectations>=0.18.0

# Testing (these won't be in production, but useful for dev)
pytest>=7.4.0
pytest-spark>=0.6.0
```

---

## Cloud Storage Connectors {#cloud-connectors}

### Google Cloud Storage (GCS)

```dockerfile
# Dockerfile snippet for GCS
ARG GCS_CONNECTOR_VERSION=hadoop3-2.2.19
RUN curl -fSL -o /opt/spark/jars/gcs-connector.jar \
    "https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/${GCS_CONNECTOR_VERSION}/gcs-connector-${GCS_CONNECTOR_VERSION}-shaded.jar"
```

**Spark configuration:**
```properties
spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS
spark.hadoop.google.cloud.auth.service.account.enable=true
```

**Usage:**
```python
df = spark.read.parquet("gs://my-bucket/data/")
df.write.parquet("gs://my-bucket/output/")
```

### Amazon S3

```dockerfile
# Dockerfile snippet for S3
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_VERSION=1.12.367
RUN curl -fSL -o /opt/spark/jars/hadoop-aws.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar" && \
    curl -fSL -o /opt/spark/jars/aws-java-sdk-bundle.jar \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar"
```

**Spark configuration:**
```properties
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.WebIdentityTokenCredentialsProvider
```

**Usage:**
```python
df = spark.read.parquet("s3a://my-bucket/data/")
```

### Azure Data Lake Storage Gen2 (ADLS)

```dockerfile
# Dockerfile snippet for ADLS
ARG HADOOP_AZURE_VERSION=3.3.4
RUN curl -fSL -o /opt/spark/jars/hadoop-azure.jar \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/${HADOOP_AZURE_VERSION}/hadoop-azure-${HADOOP_AZURE_VERSION}.jar"
```

**Usage:**
```python
df = spark.read.parquet("abfss://container@storage.dfs.core.windows.net/data/")
```

---

## Table Formats: Delta Lake, Iceberg, Hudi {#table-formats}

### Comparison

| Feature | Delta Lake | Iceberg | Hudi |
|---------|------------|---------|------|
| ACID Transactions | ✅ | ✅ | ✅ |
| Time Travel | ✅ | ✅ | ✅ |
| Schema Evolution | ✅ | ✅ | ✅ |
| Streaming Support | ✅ Excellent | ✅ Good | ✅ Excellent |
| Merge Performance | ✅ Fast | ⚠️ Moderate | ✅ Fast |
| Adoption | High (Databricks) | Growing (Netflix) | Moderate (Uber) |
| Best For | General purpose | Analytics | Incremental CDC |

### Delta Lake Setup

```dockerfile
ARG DELTA_VERSION=3.1.0
RUN curl -fSL -o /opt/spark/jars/delta-spark.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/${DELTA_VERSION}/delta-spark_2.12-${DELTA_VERSION}.jar" && \
    curl -fSL -o /opt/spark/jars/delta-storage.jar \
    "https://repo1.maven.org/maven2/io/delta/delta-storage/${DELTA_VERSION}/delta-storage-${DELTA_VERSION}.jar"
```

**Spark configuration:**
```properties
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
```

**Usage:**
```python
# Write Delta table
df.write.format("delta").save("gs://bucket/delta-table")

# Read with time travel
spark.read.format("delta").option("versionAsOf", 5).load("gs://bucket/delta-table")
```

### Apache Iceberg Setup

```dockerfile
ARG ICEBERG_VERSION=1.4.3
RUN curl -fSL -o /opt/spark/jars/iceberg-spark-runtime.jar \
    "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"
```

**Spark configuration:**
```properties
spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.iceberg.type=hadoop
spark.sql.catalog.iceberg.warehouse=gs://bucket/iceberg-warehouse
```

---

## Container Registries: GCR, ECR, ACR {#registries}

### Google Container Registry (GCR) / Artifact Registry

```bash
# Authenticate
gcloud auth configure-docker

# Build and tag
docker build -t spark-prod:3.5.0 -f Dockerfile.spark .
docker tag spark-prod:3.5.0 gcr.io/${PROJECT_ID}/spark:3.5.0
docker tag spark-prod:3.5.0 gcr.io/${PROJECT_ID}/spark:latest

# Push
docker push gcr.io/${PROJECT_ID}/spark:3.5.0
docker push gcr.io/${PROJECT_ID}/spark:latest

# For Artifact Registry (newer):
docker tag spark-prod:3.5.0 ${REGION}-docker.pkg.dev/${PROJECT_ID}/spark-images/spark:3.5.0
docker push ${REGION}-docker.pkg.dev/${PROJECT_ID}/spark-images/spark:3.5.0
```

### Amazon ECR

```bash
# Authenticate
aws ecr get-login-password --region ${REGION} | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

# Create repository (one-time)
aws ecr create-repository --repository-name spark --region ${REGION}

# Build, tag, push
docker build -t spark-prod:3.5.0 -f Dockerfile.spark .
docker tag spark-prod:3.5.0 ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/spark:3.5.0
docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/spark:3.5.0
```

### Azure Container Registry (ACR)

```bash
# Authenticate
az acr login --name ${REGISTRY_NAME}

# Build, tag, push
docker build -t spark-prod:3.5.0 -f Dockerfile.spark .
docker tag spark-prod:3.5.0 ${REGISTRY_NAME}.azurecr.io/spark:3.5.0
docker push ${REGISTRY_NAME}.azurecr.io/spark:3.5.0
```

---

## Image Optimization Strategies {#optimization}

### 1. Use Multi-Stage Builds (Already Covered)

Reduces image size by 30-50%.

### 2. Minimize Layers

```dockerfile
# Bad: Multiple RUN commands = multiple layers
RUN apt-get update
RUN apt-get install -y curl
RUN apt-get clean

# Good: Single RUN command = one layer
RUN apt-get update && \
    apt-get install -y curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*
```

### 3. Order Instructions by Change Frequency

```dockerfile
# Put rarely-changing instructions first (better caching)
FROM apache/spark:3.5.0-python3

# 1. System packages (rarely change)
RUN apt-get update && apt-get install -y ...

# 2. JARs (change occasionally)
COPY --from=jar-downloader /jars/*.jar /opt/spark/jars/

# 3. Python dependencies (change more often)
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt

# 4. Application code (changes frequently)
COPY src/ /opt/spark/work-dir/
```

### 4. Use .dockerignore

```dockerignore
# .dockerignore
.git
.gitignore
*.md
.DS_Store
__pycache__
*.pyc
*.egg-info
.pytest_cache
.venv
node_modules
*.log
```

### 5. Size Comparison

| Image | Size | Notes |
|-------|------|-------|
| `apache/spark:3.5.0-python3` | 950MB | Base only |
| Naive custom build | 1.8GB | All packages in single stage |
| Multi-stage optimized | 1.2GB | 33% smaller |

---

## Security Scanning & Best Practices {#security}

### 1. Scan Images with Trivy

```bash
# Install Trivy
brew install trivy

# Scan your image
trivy image spark-prod:3.5.0

# Output example:
# spark-prod:3.5.0 (ubuntu 22.04)
# Total: 12 (HIGH: 3, CRITICAL: 0)
```

### 2. Use Non-Root User

```dockerfile
# Already done in official Spark image
USER spark  # UID 185
```

### 3. Remove Unnecessary Tools

```dockerfile
RUN apt-get remove -y --purge curl wget && \
    apt-get autoremove -y
```

### 4. Pin Versions

```dockerfile
# Good: Pinned versions
FROM apache/spark:3.5.0-python3
ARG DELTA_VERSION=3.1.0

# Bad: Latest tags
FROM apache/spark:latest  # Don't do this
```

---

## CI/CD Pipeline for Image Builds {#cicd}

### GitHub Actions

```yaml
# .github/workflows/build-spark-image.yml
name: Build Spark Image

on:
  push:
    branches: [main]
    paths:
      - 'docker/**'
      - '.github/workflows/build-spark-image.yml'

env:
  PROJECT_ID: ${{ secrets.GCP_PROJECT_ID }}
  IMAGE_NAME: spark
  REGION: us-central1

jobs:
  build:
    runs-on: ubuntu-latest
    
    steps:
      - uses: actions/checkout@v4
      
      - name: Set up Docker Buildx
        uses: docker/setup-buildx-action@v3
      
      - name: Authenticate to GCP
        uses: google-github-actions/auth@v2
        with:
          credentials_json: ${{ secrets.GCP_SA_KEY }}
      
      - name: Configure Docker for GCR
        run: gcloud auth configure-docker
      
      - name: Build and Push
        uses: docker/build-push-action@v5
        with:
          context: ./docker
          file: ./docker/Dockerfile.spark
          push: true
          tags: |
            gcr.io/${{ env.PROJECT_ID }}/${{ env.IMAGE_NAME }}:${{ github.sha }}
            gcr.io/${{ env.PROJECT_ID }}/${{ env.IMAGE_NAME }}:latest
          cache-from: type=gha
          cache-to: type=gha,mode=max
      
      - name: Scan Image
        uses: aquasecurity/trivy-action@master
        with:
          image-ref: gcr.io/${{ env.PROJECT_ID }}/${{ env.IMAGE_NAME }}:${{ github.sha }}
          severity: 'CRITICAL,HIGH'
```

---

## Troubleshooting Common Issues {#troubleshooting}

### Issue 1: JAR Conflicts

**Symptom:** `NoSuchMethodError` or `ClassNotFoundException` at runtime.

**Cause:** Version mismatch between JARs.

**Solution:**
```bash
# Check what's in the image
docker run --rm spark-prod:3.5.0 ls -la /opt/spark/jars/ | grep hadoop

# Ensure consistent versions
# Example: Hadoop 3.3.4 everywhere
```

### Issue 2: Out of Memory During Build

**Symptom:** `docker build` fails with memory error.

**Solution:**
```bash
# Increase Docker memory limit
# Docker Desktop → Settings → Resources → Memory → 8GB+

# Or build with constrained resources
docker build --memory=4g -t spark-prod:3.5.0 .
```

### Issue 3: Image Won't Pull in K8s

**Symptom:** `ImagePullBackOff` in pod status.

**Solution:**
```bash
# Check image exists
gcloud container images list-tags gcr.io/${PROJECT_ID}/spark

# Check K8s has access
kubectl get secrets -n spark-jobs | grep regcred

# For GKE with Workload Identity, ensure node service account has access
```

### Issue 4: Python Package Not Found

**Symptom:** `ModuleNotFoundError` in PySpark job.

**Solution:**
```dockerfile
# Verify virtual environment is active
ENV PATH="/opt/venv/bin:$PATH"
ENV PYSPARK_PYTHON=/opt/venv/bin/python

# Check packages are installed
docker run --rm spark-prod:3.5.0 pip list
```

---

## Summary

You now have:

✅ **Understanding** of Spark base images and their contents  
✅ **Production Dockerfile** with multi-stage builds  
✅ **Cloud connectors** for GCS, S3, and ADLS  
✅ **Table formats** - Delta Lake, Iceberg, Hudi  
✅ **Python dependencies** properly managed  
✅ **Monitoring** with Prometheus JMX exporter  
✅ **CI/CD pipeline** for automated builds  
✅ **Security** scanning and best practices  

---

## What's Next

In **Part 4: Security Deep Dive**, we'll cover:
- Complete RBAC setup with least privilege
- GCP Workload Identity in depth
- AWS IRSA configuration
- Network Policies for pod isolation
- Secrets management patterns
- Pod Security Standards

---

*Next: [Part 4: Security Deep Dive →](./part-04-security.md)*

*Previous: [Part 2: Environment Setup ←](./part-02-environment-setup.md)*
