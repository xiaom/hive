# Docker Usage Instruction

The Dockerfile in the project directory provides an easier way to paly with HiveServer2 with compressor enabled.

Assume you have already built with hive locally with

```
mvn clean install -Phadoop-2,dist -DskipTests
```



## Build the image

Run the following command to build the hive image with compressor support:

```
# Download hadoop jar files locally to docker-root/opt
#
# The reason is to  avoid downloading it every time when docker tries
# to rebuild a imaage.
./docker-download

# Now build the docker image
./docker-build
```


## Start the container

Start a contianer:

```
docker-compose up
```

It will start a container named `hive_hive_1`. You can also check which containers are running with

```
docker-compose ps
```

## Use the container

You can use the container using `docker exec` command, which is with the following format.

```
Usage: docker exec [OPTIONS] CONTAINER COMMAND [ARG...]

Run a command in a running container

  -d, --detach=false         Detached mode: run command in the background
  -i, --interactive=false    Keep STDIN open even if not attached
  -t, --tty=false            Allocate a pseudo-TTY
```

Thus, we can run any command under `$HIVE_HOME/bin` by
adding `docker exec hive_hive_1 COMMAND`

e.g.,

```
docker exec hive_hive_1 hive -e "show tables"
```

## Load data into the container

The project directory 'share/' is mounted to docker container '/share'.
To load data into containers, write scripts and put data under 'share' directory.
Then, you can run it from the container as you want.

To give an example, we add `load_Integer_Table.hql` and `Integer_Table.csv` under the `share/` directory.

```
docker exec hive_hive_1 hive -f /share/load_Integer_Table.hql
docker exec hive_hive_1 hive -e "select * from Integer_table"
```
