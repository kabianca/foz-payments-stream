# Spark driver image for the Foz streaming job.
#
# The Delta and Kafka connector jars are resolved once, at build time, and
# copied onto Spark's classpath. Resolving them at start-up with --packages
# would need Maven Central every time the container restarts, and the whole
# point of this job is that it restarts.
ARG SPARK_VERSION=4.1.3
FROM apache/spark:${SPARK_VERSION}-scala2.13-java17-python3-ubuntu

ARG SPARK_VERSION
ARG DELTA_VERSION=4.4.0
# Delta 4.x publishes one artifact per Spark minor (delta-spark_4.1_2.13); the
# unsuffixed one targets the newest Spark and fails at runtime on older ones.
ARG SPARK_MINOR=4.1

USER root

RUN /opt/spark/bin/spark-submit \
      --packages io.delta:delta-spark_${SPARK_MINOR}_2.13:${DELTA_VERSION},org.apache.spark:spark-sql-kafka-0-10_2.13:${SPARK_VERSION} \
      --conf spark.jars.ivy=/tmp/ivy \
      --class org.apache.spark.examples.SparkPi \
      /opt/spark/examples/jars/spark-examples_2.13-${SPARK_VERSION}.jar 1 > /dev/null \
 && cp /tmp/ivy/jars/*.jar /opt/spark/jars/ \
 && rm -rf /tmp/ivy

# delta-spark's Python side only; PySpark itself ships with the image.
RUN pip install --no-cache-dir --no-deps "delta-spark==${DELTA_VERSION}" importlib_metadata

ENV PYTHONPATH=/opt/foz \
    PYTHONUNBUFFERED=1 \
    TZ=UTC \
    HOME=/tmp

WORKDIR /opt/foz
COPY foz/ /opt/foz/foz/
COPY docker/stream-entrypoint.sh /opt/foz/entrypoint.sh
COPY docker/log4j2.properties /opt/spark/conf/log4j2.properties

USER 185
ENTRYPOINT ["/opt/foz/entrypoint.sh"]
CMD ["--master", "local[2]", \
     "--driver-memory", "1g", \
     "--conf", "spark.jars.ivy=/tmp/.ivy2", \
     "--conf", "spark.sql.shuffle.partitions=4", \
     "--conf", "spark.ui.enabled=false", \
     "--conf", "spark.driver.extraJavaOptions=-Duser.timezone=UTC", \
     "/opt/foz/foz/stream.py"]
