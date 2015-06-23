# Use phusion/baseimage as base image. To make your builds reproducible, make
# sure you lock down to a specific version, not to `latest`!
# See https://github.com/phusion/baseimage-docker/blob/master/Changelog.md for
# a list of version numbers.
FROM phusion/baseimage:0.9.16

# Use baseimage-docker's init system.
CMD ["/sbin/my_init"]

ENV DEBIAN_FRONTEND noninteractive

COPY docker-root /
# Install build requirements
RUN apt-get update && \
  apt-get install --no-install-recommends -y wget mysql-server curl openjdk-7-jdk libmysql-java

ENV JAVA_HOME /usr/lib/jvm/java-7-openjdk-amd64

RUN mkdir -p /hive /hadoop
# ------
# Hadoop
# ------
# curl -SsfLO "http://www.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz" && \
ENV HADOOP_VERSION 2.6.0
RUN tar xzf /opt/hadoop-$HADOOP_VERSION.tar.gz --strip=1 -C /hadoop
ENV HADOOP_HOME /hadoop
RUN rm /opt/*.tar.gz

# ----
# Hive
# ----
ENV HIVE_HOME /hive
ENV HIVE_CONF $HIVE_HOME/conf
ENV HIVE_VERSION 2.0.0
WORKDIR $HIVE_HOME
ADD packaging/target/apache-hive-$HIVE_VERSION-SNAPSHOT-bin/apache-hive-$HIVE_VERSION-SNAPSHOT-bin $HIVE_HOME
RUN cp /usr/share/java/mysql-connector-java.jar $HIVE_HOME/lib/
COPY docker-root/hive /hive/conf

# Metastore
#
# refer to:
#   metastore/src/java/org/apache/hadoop/hive/metastore/MetaStoreSchemaInfo.java
# for schematool version.

# Fix the jline problem 
ENV HADOOP_USER_CLASSPATH_FIRST true

RUN sed -i 's/^\(bind-address\s.*\)/# \1/' /etc/mysql/my.cnf && \
  sed -i 's/^\(log_error\s.*\)/# \1/' /etc/mysql/my.cnf && \
  echo "mysqld_safe &" > /tmp/config && \
  echo "mysqladmin --silent --wait=30 ping || exit 1" >> /tmp/config && \
  echo "mysql -e 'CREATE USER \"dbuser\"@\"%\" IDENTIFIED BY \"dbuser\"; GRANT ALL PRIVILEGES ON *.* TO \"dbuser\"@\"%\" WITH GRANT OPTION;'" >> /tmp/config && \
  echo "bin/schematool -dbType mysql -initSchemaTo 1.2.0" >> /tmp/config && \
  echo "mysqladmin shutdown" >> /tmp/config && \
  bash /tmp/config && \
  rm -f /tmp/config

EXPOSE 10000 9083
ENV PATH=/hive/bin:${PATH}
# cleanup
RUN  apt-get clean && \
  rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*
