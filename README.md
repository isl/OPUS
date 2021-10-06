# OPUS

## How to get OPUS
##### git clone https://github.com/isl/OPUS.git

## Compile OPUS
##### First you need to install maven to build OPUS Then in the root folder where pom.xml exists, run:
##### mvn package
##### A target folder should be created if everything builds correclty with the jar inside.

## Run OPUS
##### To run OPUS you first need to install Apache Spark (version 2.2+) and Hadoop (version 2.7+). Also hdfs command (inside of bin/ in hadoop) should be set as an environment variable(be visible as hdfs inside OPUS).
##### To run each of the components of OPUS (partitioner, query translator, query executor) the appropriate script should be used.
##### Inside of scripts/ the appropriate scripts can be found. For Partitioner LAP (run_lap.sh) for query translator (run_translator.sh) and for query executor (run_query.sh).
##### Each script should be modified accordingly with the steps bellow.

### Scripts modification
##### In every script modify all fields of spark submit (master, memory,path_to_jar etc.)

### Script Arguments

##### **dataset_name:** a specific name for each dataset. (same name must be used in all procedures for a specific dataset)

##### **hdfs_path:** hdfs base folder path

##### **intance_path:** hdfs path of dataset instances (.nt)

##### **schema_path:** hdfs path of dataset schema (triples format)

##### **sparql_input_folder:** input folder of sparql queries

### To partition data using LAP use the script run_lap like this:
##### ./run_lap.sh dataset_name hdfs_path instance_path schema_path

### To translate sparql queries use the script run_translator like this:
##### ./run_translator.sh dataset_name hdfs_path sparql_input_folder

### To execute the translated queries use the script run_translator like this:
##### ./run_query.sh  dataset_name hdfs_path translated_queries_folder
