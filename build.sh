ps aux | awk '/[h]ive/ { print $2 }' | xargs kill
mvn clean install -Phadoop-2,dist -DskipTests
cp /opt/hive/mysql-connector-java-5.1.34/mysql-connector-java-5.1.34-bin.jar packaging/target/apache-hive-0.14.0-SIMBA-bin/apache-hive-0.14.0-SIMBA-bin/lib

