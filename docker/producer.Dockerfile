FROM python:3.12-slim

ARG CONFLUENT_KAFKA_VERSION=2.15.1
RUN pip install --no-cache-dir "confluent-kafka==${CONFLUENT_KAFKA_VERSION}"

ENV PYTHONPATH=/opt/foz \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

WORKDIR /opt/foz
COPY foz/ /opt/foz/foz/

ENTRYPOINT ["python", "-m", "foz.producer"]
