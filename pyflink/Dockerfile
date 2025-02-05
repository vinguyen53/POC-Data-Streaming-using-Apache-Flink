FROM apache/flink:latest

# install python3 and pip3
RUN apt-get update -y && \
apt-get install -y python3 python3-pip python3-dev && rm -rf /var/lib/apt/lists/*
RUN ln -s /usr/bin/python3 /usr/bin/python

# install PyFlink

COPY apache-flink*.tar.gz /
RUN pip3 install /apache-flink-libraries*.tar.gz && pip3 install /apache-flink*.tar.gz

WORKDIR /opt/flink

# dependencies

RUN curl -o /opt/flink/opt/flink-connector-kafka-3.3.0-1.20.jar https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar
RUN curl -o /opt/flink/opt/kafka-clients-3.9.0.jar https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.0/kafka-clients-3.9.0.jar

RUN mkdir ./plugins/s3-fs-hadoop && \
    cp ./opt/flink-s3-fs-hadoop-*.jar ./plugins/s3-fs-hadoop/

# RUN cp ./opt/flink-s3-fs-hadoop-*.jar ./lib/