package org.ics.isl

object Constants {
    val HDFS_BASE = "hdfs://clusternode1:9000/"
	val HDFS = "hdfs://clusternode1:9000/jagathan/"
    val RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"

    val dbpediaUri = "dbpedia"
    // val HDFS = "hdfs://127.0.0.1:9000/"
    

    //dbpedia constants
 //    val schemaPath = HDFS + "dbpedia/schema/full_schema_v2.txt"
 //    val instancePath = HDFS + "dbpedia/clean_instances/part-00000"   
	// val schemaVerticesFile = "dbpedia_data/schema_vertices"
 //    val schemaEdgesFile = "dbpedia_data/schema_edges"
 //    val instanceVerticesFile = "dbpedia_data/instance_vertices"
 //    val instanceEdgesFile = "dbpedia_data/instance_edges"
 //    val schemaCC = "dbpedia_data/schema_cc"
 //    val shortestPaths = "dbpedia_data/shortest_paths"
 //    val weightedVertices = "dbpedia_data/weighted_vertices"
 //    val weightedEdges = "dbpedia_data/weighted_edges"
 //    val schemaClusterFile = "dbpedia_data/cluster/schema_cluster" 
 //    val clusters = "dbpedia_data/cluster/clusters" 
 //    val nodeIndexFile = "dbpedia_data/node_index"
 //    val baseLine = "dbpedia_data/baseline"

 //    //Local file paths
 //    val classIndexFile = "../dbpedia_local/class_index.txt"
 //    val subjIndexRoot = "../dbpedia_local/subj_index"
 //    val objIndexRoot = "../dbpedia_local/obj_index"

 //    val finalClassIndexFile = "../dbpedia_local/final_class_index.txt"
 //    val statisticsFile = "../dbpedia_local/statistics.txt"
 //    val partitionStatFile = "../dbpedia_local/partition_stat.txt"
 //    val dataStatisticsFile = "../dbpedia_local/data_statistics.txt"
 //    val schemaNodeFrequency = "../dbpedia_local/schema_node_freq.txt"
 //    val huaBCFile = "../dbpedia_local/hua_bc.txt"
 //    val edmondsBCFile = "../dbpedia_local/edmonds_bc.txt"
 //    val schemaImportance = "../dbpedia_local/schema_node_importance.txt"
 //    val centroidMapFile = "../dbpedia_local/centroid_map.txt"
 //   val schemaNodeCount = "../dbpedia_local/schema_node_count.txt"




    // SWDF Constants

    // val schemaPath = HDFS + "swdf/schema.txt"
    // val instancePath = HDFS + "swdf/instances/clean_swdf.nt"

    // //HDFS paths
    // val schemaVerticesFile = "swdf_data/schema_vertices"
    // val schemaEdgesFile = "swdf_data/schema_edges"
    // val instanceVerticesFile = "swdf_data/instance_vertices"
    // val instanceEdgesFile = "swdf_data/instance_edges"
    // val schemaCC = "swdf_data/schema_cc"
    // val shortestPaths = "swdf_data/shortest_paths"
    // val weightedVertices = "swdf_data/weighted_vertices"
    // val weightedEdges = "swdf_data/weighted_edges"
    // val schemaClusterFile = "swdf_data/cluster/schema_cluster" 
    // val clusters = "swdf_data/cluster/clusters" 
    // val nodeIndexFile = "swdf_data/node_index"
    // val baseLine = "swdf_data/baseline"

    // //Local file paths
    // val classIndexFile = "../swdf_local/class_index.txt"
    // val finalClassIndexFile = "../swdf_local/final_class_index.txt"
    // val statisticsFile = "../swdf_local/statistics.txt"
    // val partitionStatFile = "../swdf_local/partition_stat.txt"
    // val dataStatisticsFile = "../swdf_local/data_statistics.txt"
    // val schemaNodeFrequency = "../swdf_local/schema_node_freq.txt"
    // val huaBCFile = "../swdf_local/hua_bc.txt"
    // val edmondsBCFile = "../swdf_local/edmonds_bc.txt"
    // val schemaImportance = "../swdf_local/schema_node_importance.txt"
    // val centroidMapFile = "../swdf_local/centroid_map.txt"
    // val subjIndexRoot = "../swdf_local/subj_index"
    // val objIndexRoot = "../swdf_local/obj_index"
    // val schemaNodeCount = "../swdf_local/schema_node_count.txt"



    //LUBM Constants

    val schemaPath = HDFS + "lubm-1000/clean_lubm_schema.txt"
    val instancePath = HDFS + "lubm-1000/clean_lubm.nt"  

    val schemaVerticesFile = "lubm-1000_data/schema_vertices"
    val schemaEdgesFile = "lubm-1000_data/schema_edges"
    val instanceVerticesFile = "lubm-1000_data/instance_vertices"
    val instanceEdgesFile = "lubm-1000_data/instance_edges"
    val schemaCC = "lubm-1000_data/schema_cc"
    val shortestPaths = "lubm-1000_data/shortest_paths"
    val weightedVertices = "lubm-1000_data/weighted_vertices"
    val weightedEdges = "lubm-1000_data/weighted_edges"
    val schemaClusterFile = "lubm-1000_data/cluster/schema_cluster" 
    val clusters = "lubm-1000_data/cluster/clusters" 
    val nodeIndexFile = "lubm-1000_data/node_index"
    val baseLine = "lubm-1000_data/baseline"

    //Local file paths
    val classIndexFile = "../lubm-1000_local/class_index.txt"
    val subjIndexRoot = "../lubm-1000_local/subj_index"
    val objIndexRoot = "../lubm-1000_local/obj_index"

    val finalClassIndexFile = "../lubm-1000_local/final_class_index.txt"
    val statisticsFile = "../lubm-1000_local/statistics.txt"
    val partitionStatFile = "../lubm-1000_local/partition_stat.txt"
    val dataStatisticsFile = "../lubm-1000_local/data_statistics.txt"
    val schemaNodeFrequency = "../lubm-1000_local/schema_node_freq.txt"
    val huaBCFile = "../lubm-1000_local/hua_bc.txt"
    val edmondsBCFile = "../lubm-1000_local/edmonds_bc.txt"
    val schemaImportance = "../lubm-1000_local/schema_node_importance.txt"
    val centroidMapFile = "../lubm-1000_local/centroid_map.txt"
    val schemaNodeCount = "../lubm-1000_local/schema_node_count.txt"


    //LUBM 10K Constants

    // val schemaPath = HDFS + "lubm-10k/lubm_schema.txt"
    // val instancePath = HDFS + "lubm-10k/Universities-clean.nt"  

    // val schemaVerticesFile = "lubm-10k_data/schema_vertices"
    // val schemaEdgesFile = "lubm-10k_data/schema_edges"
    // val instanceVerticesFile = "lubm-10k_data/instance_vertices"
    // val instanceEdgesFile = "lubm-10k_data/instance_edges"
    // val schemaCC = "lubm-10k_data/schema_cc"
    // val shortestPaths = "lubm-10k_data/shortest_paths"
    // val weightedVertices = "lubm-10k_data/weighted_vertices"
    // val weightedEdges = "lubm-10k_data/weighted_edges"
    // val schemaClusterFile = "lubm-10k_data/cluster/schema_cluster" 
    // val clusters = "lubm-10k_data/cluster/clusters" 
    // val nodeIndexFile = "lubm-10k_data/node_index"
    // val baseLine = "lubm-10k_data/baseline"

    // //Local file paths
    // val classIndexFile = "../lubm-10k_local/class_index.txt"
    // val subjIndexRoot = "../lubm-10k_local/subj_index"
    // val objIndexRoot = "../lubm-10k_local/obj_index"

    // val finalClassIndexFile = "../lubm-10k_local/final_class_index.txt"
    // val statisticsFile = "../lubm-10k_local/statistics.txt"
    // val partitionStatFile = "../lubm-10k_local/partition_stat.txt"
    // val dataStatisticsFile = "../lubm-10k_local/data_statistics.txt"
    // val schemaNodeFrequency = "../lubm-10k_local/schema_node_freq.txt"
    // val huaBCFile = "../lubm-10k_local/hua_bc.txt"
    // val edmondsBCFile = "../lubm-10k_local/edmonds_bc.txt"
    // val schemaImportance = "../lubm-10k_local/schema_node_importance.txt"
    // val centroidMapFile = "../lubm-10k_local/centroid_map.txt"
    // val schemaNodeCount = "../lubm-10k_local/schema_node_count.txt"

    // Watdiv Constants

    // val schemaPath = HDFS + "watdiv/watdiv_schema.txt"
    // val instancePath = HDFS + "watdiv/watdiv.10M.nt"   

    // val schemaVerticesFile = "watdiv_generated_data/schema_vertices"
    // val schemaEdgesFile = "watdiv_generated_data/schema_edges"
    // val instanceVerticesFile = "watdiv_generated_data/instance_vertices"
    // val instanceEdgesFile = "watdiv_generated_data/instance_edges"
    // val schemaCC = "watdiv_generated_data/schema_cc"
    // val huaBCFile = "watdiv_generated_data/hua_bc"
    // val edmondsBCFile = "watdiv_generated_data/edmonds_bc"
    // val cardinalities = "watdiv_generated_data/cardinalities"
    // val shortestPaths = "watdiv_generated_data/shortest_paths"
    // val weightedVertices = "watdiv_generated_data/weighted_vertices"
    // val weightedEdges = "watdiv_generated_data/weighted_edges"
    // val schemaClusterFile = "watdiv_generated_data/cluster/schema_cluster" 
    // val finalClusters = "watdiv_generated_data/cluster/final_clusters"
    // val firstStageClusters = "watdiv_generated_data/cluster/first_stage_clusters"
    // val nonClusteredLiterals = "watdiv_generated_data/cluster/non_clustered_literals"
    // val nonClusteredTriples = "watdiv_generated_data/cluster/non_clustered_triples"
    // val schemaImportance = "watdiv_generated_data/schema_node_importance"
    // val clusters = "watdiv_generated_data/cluster/clusters"
    // val schemaNodeFrequency = "watdiv_generated_data/schema_node_frequency"
    // val nodeIndexFile = "watdiv_generated_data/node_index"
    // val subjIndexFile = "watdiv_generated_data/subj_index"
    // val objIndexFile = "watdiv_generated_data/obj_index"
    // val typeQueries = "/queries/test_type_queries"
    // val parquetSuffix = "-2ffe5963-ec64-41f9-b05d-40e6b3d3a72b-c000.snappy.parquet"
    // val baseLine = "watdiv_generated_data/baseline"

    // //Local file paths
    // val classIndexFile = "watdiv_metadata/class_index.txt"
    // val predicateIndexFile = "watdiv_metadata/predicate_index.txt"
    // val statisticsFile = "watdiv_metadata/statistics.txt"
    // val dataStatisticsFile = "watdiv_metadata/data_statistics.txt"
    // val nodeIndexFileLocal = "watdiv_metadata/node_index.txt"
    // val subjIndexFileLocal = "watdiv_metadata/subj_index.txt"
    // val objIndexFileLocal = "watdiv_metadata/obj_index.txt"

}
