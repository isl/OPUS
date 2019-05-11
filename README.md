# LAWA

## Script Arguments
### __num_partitions:__ the number of partitions that we want to split the dataset

### **dataset_name:** a specific name for each dataset. (same name must be used in all procedures for a specific dataset)

### **hdfs_path:** hdfs base folder path

### **intance_path:** hdfs path of dataset instances (.nt)

### **schema_path:** hdfs path of dataset schema (triples format)

### **sparql_input_folder:** input folder of sparql queries

### **balance:** 0 for lap partitioning 1 for BLAP

## To run LAP use the script run_lap like this:
#### ./run_lap.sh num_partitions dataset_name hdfs_path instance_path schema_path

## To run BLAP use the script run_blap like this:
#### ./run_blap.sh num_partitions dataset_name hdfs_path instance_path schema_path

## To run translator use the script run_translator like this:
#### ./run_translator.sh num_partitions dataset_name balance hdfs_path sparql_input_folder
