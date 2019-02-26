# LAWA
# num_partitions the number of partitions that we want to split the dataset
# dataset_name a specific name for each dataset. (same name must be used in all procedures for a specific dataset)
# To run LAP partitioner:
./run_lap.sh num_partitions dataset_name

# To run BLAP partitioner:
./run_blap.sh num_partitions dataset_name

# blap is 1 if data are partitioned using BLAP, 0 if with LAP
# To translate queries
./run_translator.sh num_partitions dataset_name blap

# To run query
./run_query.sh num_partitions dataset_name blap
