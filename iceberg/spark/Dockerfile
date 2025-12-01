# Use Java 17 (Temurin)
FROM eclipse-temurin:17-jdk-jammy

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-setuptools \
    python3-distutils \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN pip install --no-cache-dir \
    pyspark \
    py4j \
    boto3 \
    psycopg2-binary


ENV SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    ICEBERG_VERSION=1.6.1

RUN curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    -o /tmp/spark.tgz \
    && tar -xzf /tmp/spark.tgz -C /opt \
    && rm /tmp/spark.tgz

ENV SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}
ENV PATH="$SPARK_HOME/bin:$PATH"

RUN mkdir -p /opt/iceberg-jars

RUN curl -L "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
    -o /opt/iceberg-jars/iceberg-spark-runtime.jar

RUN curl -L "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" \
    -o /opt/iceberg-jars/postgresql-jdbc.jar

ENV SPARK_EXTRA_JARS=/opt/iceberg-jars/iceberg-spark-runtime.jar,/opt/iceberg-jars/postgresql-jdbc.jar
ENV SPARK_CLASSPATH=$SPARK_EXTRA_JARS

# -------------------------------------------------------------------
COPY compact_iceberg.py /app/compact_iceberg.py
COPY run-compaction.sh /app/run-compaction.sh
RUN chmod +x /app/run-compaction.sh

ENV PYTHONPATH=/app

# Default envs (override at runtime)
# ENV PG_CONN_STRING="jdbc:postgresql://postgres:5432/iceberg_catalog" \
#     PG_USER="postgres" \
#     PG_PASSWORD="postgres" \
#     CATALOG_NAME="my_iceberg_catalog" \
#     CATALOG_ROOT="s3://trinity-pilot/warehouse/" \
#     S3_ACCESS_KEY_ID="***" \
#     S3_SECRET_ACCESS_KEY="***" \
#     S3_REGION="us-east-1" \
#     TARGET_FILE_SIZE_BYTES=536870912 \
#     MAX_PARALLELISM=4

ENTRYPOINT ["/app/run_compaction.sh"]
