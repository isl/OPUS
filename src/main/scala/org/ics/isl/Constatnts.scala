package org.ics.isl

object Constants {
    // val HDFS_BASE = "hdfs://clusternode1:9000/"
	// val HDFS = "hdfs://clusternode1:9000/jagathan/"
    val RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
    val dbpediaUri = "dbpedia"
  
    val schemaVerticesFile = "_data/schema_vertices"
    val schemaEdgesFile = "_data/schema_edges"
    val instanceVerticesFile = "_data/instance_vertices"
    val instanceEdgesFile = "_data/instance_edges"
    val schemaCC = "_data/schema_cc"
    val shortestPaths = "_data/shortest_paths"
    val weightedVertices = "_data/weighted_vertices"
    val weightedEdges = "_data/weighted_edges"
    val schemaClusterFile = "_data/cluster/schema_cluster" 
    val clusters = "_data/cluster/clusters" 
    val nodeIndexFile = "_data/node_index"
    val baseLine = "_data/baseline"

    //Local file paths
    val classIndexFile = "_local/class_index.txt"
    val subjIndexRoot = "_local/subj_index"
    val objIndexRoot = "_local/obj_index"

    val finalClassIndexFile = "_local/final_class_index.txt"
    val statisticsFile = "_local/statistics.txt"
    val partitionStatFile = "_local/partition_stat.txt"
    val dataStatisticsFile = "_local/data_statistics.txt"
    val schemaNodeFrequency = "_local/schema_node_freq.txt"
    val huaBCFile = "_local/hua_bc.txt"
    val edmondsBCFile = "_local/edmonds_bc.txt"
    val schemaImportance = "_local/schema_node_importance.txt"
    val centroidMapFile = "_local/centroid_map.txt"
    val schemaNodeCount = "_local/schema_node_count.txt"

}