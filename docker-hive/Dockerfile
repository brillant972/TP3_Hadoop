FROM openjdk:8-jdk

# Install Hive
ENV HIVE_VERSION=4.0.1
ENV HIVE_HOME=/opt/hive
ENV PATH=$PATH:$HIVE_HOME/bin
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin

RUN apt-get update && apt-get install -y wget tar procps net-tools

RUN wget https://downloads.apache.org/hive/hive-${HIVE_VERSION}/apache-hive-${HIVE_VERSION}-bin.tar.gz \
    && tar -xvzf apache-hive-${HIVE_VERSION}-bin.tar.gz \
    && mv apache-hive-${HIVE_VERSION}-bin /opt/hive \
    && rm apache-hive-${HIVE_VERSION}-bin.tar.gz

RUN wget https://downloads.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz \
    && tar -xvzf hadoop-3.3.6.tar.gz \
    && mv hadoop-3.3.6 /opt/hadoop \
    && rm hadoop-3.3.6.tar.gz

COPY core-site.xml $HIVE_HOME/conf/
COPY hive-site.xml $HIVE_HOME/conf/

WORKDIR /opt/hive
CMD ["bin/hive"]
