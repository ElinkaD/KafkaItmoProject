FROM flink:1.19.2-scala_2.12-java17

ARG FLINK_KAFKA_CONNECTOR_VERSION=3.2.0-1.19
ARG FLINK_FORMATS_VERSION=1.19.2
ARG FLINK_SHADED_HADOOP_VERSION=2.8.3-10.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir apache-flink==1.19.2 pandas

RUN cp /opt/flink/opt/flink-python-1.19.2.jar /opt/flink/lib/
RUN curl -fL "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${FLINK_KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar" \
    -o /opt/flink/lib/flink-sql-connector-kafka-${FLINK_KAFKA_CONNECTOR_VERSION}.jar
RUN curl -fL "https://repo1.maven.org/maven2/org/apache/flink/flink-parquet/${FLINK_FORMATS_VERSION}/flink-parquet-${FLINK_FORMATS_VERSION}.jar" \
    -o /opt/flink/lib/flink-parquet-${FLINK_FORMATS_VERSION}.jar
RUN curl -fL "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-parquet/${FLINK_FORMATS_VERSION}/flink-sql-parquet-${FLINK_FORMATS_VERSION}.jar" \
    -o /opt/flink/lib/flink-sql-parquet-${FLINK_FORMATS_VERSION}.jar
RUN curl -fL "https://repo1.maven.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/${FLINK_SHADED_HADOOP_VERSION}/flink-shaded-hadoop-2-uber-${FLINK_SHADED_HADOOP_VERSION}.jar" \
    -o /opt/flink/lib/flink-shaded-hadoop-2-uber-${FLINK_SHADED_HADOOP_VERSION}.jar
