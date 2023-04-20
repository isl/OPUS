# DIAERESIS
DIAERESIS is a novel platform that presents a new partitioning technique of RDF dataset for SPARK, which has been designed specifically for improving query answering efficiency by reducing data access at query answering. 

Specifically, the input RDF dataset is fed to the DIAERESIS Partitioner in order to partition it. The Partitioner uses the Dependency Aware Partitioning (DAP) algorithm in order to construct the first-level partitions of data focusing on the structure of the RDF dataset and the dependence between the schema nodes. In the sequel, based on this first-level partitioning, instances are assigned to the various partitions, and the vertical partitions are created and stored in the HDFS. Along with the partitions and vertical partitions, the necessary indexes are produced as well. 

The Query Processor module of DIAERESIS is implemented on top of Spark. An input SPARQL query is parsed and then translated into an SQL query automatically. To achieve this, first, the Query Processor detects the first-level and vertical partitions that should be accessed for each triple pattern in the query, creating a Query Partitioning Information Structure. This procedure is called partition discovery. Then, this Query Partitioning Information Structure is used by the Query Translator of DIAERESIS, to construct the final SQL query. Our approach translates the SPARQL query into SQL in order to benefit from the Spark SQL interface and its native optimizer which is enhanced to offer better results. Finally, the Query Processor executes the translated query returning the appropriated results of the query (number of results and information about the execution).

## How to get DIAERESIS
```
git clone https://github.com/isl/DIAERESIS.git
```


## Compile DIAERESIS
First you need to install maven to build DIAERESIS Then in the root folder where pom.xml exists, run:
```
mvn package
```

A target folder should be created if everything builds correctly with the jar inside.

## Run DIAERESIS

To run DIAERESIS you first need to install Apache Spark (version 2.2+) and Hadoop (version 2.7+). Also hdfs command (inside of bin/ in hadoop) should be set as an environment variable(be visible as hdfs inside DIAERESIS).

To run each of the components of DIAERESIS (partitioner, query translator, query executor) the appropriate script should be used.

Inside of the folder #[scripts](https://github.com/isl/DIAERESIS/tree/master/scripts)# the appropriate scripts can be found. For partitioner DAP (run_dap.sh), for query translator (run_translator.sh) and for query executor (run_query.sh).

Each script should be modified accordingly with the steps bellow. In every script modify all fields of spark submit (master, memory, path_to_jar etc.)


### Script Arguments

* **dataset_name:** a specific name for each dataset. (same name must be used in all procedures for a specific dataset)

* **partition_num:** the number of partitions formulated each dataset. (same number must be used in all procedures for a specific dataset)

* **hdfs_path:** hdfs base folder path

* **intance_path:** hdfs path of dataset instances (.nt)

* **schema_path:** hdfs path of dataset schema (triples format)

* **sparql_input_folder:** input folder of sparql queries

* **translated_queries_folder:** the folder with the result SQL queries translated from the input sparql queries

### Partition data with DAP usinng the script run_dap like this:
```
./run_dap.sh dataset_name partition_num hdfs_path instance_path schema_path
```
### Translate sparql queries using the script run_translator like this:
```
./run_translator.sh dataset_name partition_num hdfs_path sparql_input_folder
```
### Execute the translated queries using the script run_query like this:
```
./run_query.sh  dataset_name partition_num hdfs_path translated_queries_folder
```

## Dataset
All the information about the datasets used in our experimental evaluation can be found below.

* We utilize the [LUBM](http://swat.cse.lehigh.edu/projects/lubm/) synthetic data generator (UBA1.7) to create the following datasets:

	* **LUBM-100** with 100 universities 
	* **LUBM-1300** with 1300 universities
	* **LUBM-23000** with 2300 universities
		* [Download Schema of LUBM-100-1300-2300](https://users.ics.forth.gr/~kondylak/SWJ/lubm100_1300_2300.zip)
	* **LUBM-10240** with 10240 universities
		* [Download Schema of LUBM-10240](https://users.ics.forth.gr/~kondylak/SWJ/lubm10240.zip)
   		* [Download Data Dump of LUBM-10240](https://www.dropbox.com/s/4ifouv5n5pa4vdk/10240_new_str.tar.gz?dl=0)
* **SWDF**
	* [Download Schema & Data Dump of SWDF](https://users.ics.forth.gr/~kondylak/SWJ/swdf.zip)
* **DBpedia**
 	* [Download Schema of DBpedia](https://users.ics.forth.gr/~kondylak/SWJ/dbpedia.zip)
	* [Download Data Dump of DBpedia](https://drive.google.com/file/d/1QX1-if3oa1fykJ7zJvC3l8ZwflvTeQce/view?usp=share_link)

## Benchmark Queries 
All queries used in our experimental evaluation exists in #[queries](https://github.com/isl/DIAERESIS/tree/master/queries)# folder including the individual benchmark queries.

#### Individual Queries
* [LUBM-100](https://github.com/isl/DIAERESIS/tree/master/queries/lubm100_1300_2300)
* [LUBM-1300](https://github.com/isl/DIAERESIS/tree/master/queries/lubm100_1300_2300)
* [LUBM-2300](https://github.com/isl/DIAERESIS/tree/master/queries/lubm100_1300_2300)
* [LUBM-10240](https://github.com/isl/DIAERESIS/tree/master/queries/lubm10240)
* [SWDF](https://github.com/isl/DIAERESIS/tree/master/queries/swdf)
* [DBpedia](https://github.com/isl/DIAERESIS/tree/master/queries/dbpedia)

## Contact

If you have any problems using DIAERESIS fell free to send an email.
* Georgia Troullinou (troulin@ics.forth.gr)
* Haridimos Kondylakis (kondylak@ics.forth.gr)
