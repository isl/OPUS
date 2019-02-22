package org.ics.isl

import java.io._
import sys.process._


object HdfsUtils {
	val hdfs = "/home/hdfs/hdfs/bin/hdfs"

    def hdfsExists(path: String): Boolean = {
		try{
      		val cmd = hdfs + " dfs -ls " + "/jagathan/" + path
      		val output = cmd.!!
    	} catch {
      		case e: Exception => return false
    	}
    	return true
	}

    //Does not work with * in parhs
    // def hdfsExists(spark: SparkSession, hdfsPath: String, filePath: String) = {
    //     val hadoopFs: FileSystem = FileSystem.get(new URI(hdfsPath), spark.sparkContext.hadoopConfiguration)
    //     hadoopFs.exists(new org.apache.hadoop.fs.Path(filePath))
    // }

    def moveFile(file: String, dest: String) = {
        val mkdir = hdfs + " dfs -mkdir " + "/jagathan/" + dest
        val mkdirOut = mkdir ! 
        val cmd = hdfs + " dfs -mv " + "/jagathan/" + file + " " + "/jagathan/" + dest
        val output = cmd !
    }

	def removeDirInHDFS(path: String) = {
        val cmd = hdfs + " dfs -rm -r " + "/jagathan/" + path
        val output = cmd !
    }

    def removeFilesInHDFS(path: String) = {
        val cmd = hdfs + " dfs -rm " + "/jagathan/" + path
        val output = cmd !
    }

    def countSubFolders(path: String): Int = {
        val lsCmd =  hdfs + " dfs -ls " + "/jagathan/" + path
        val cmd:String =  (lsCmd #| "wc -l").!!.trim
        (cmd.toInt - 2)
    }

    def getDirFilesNames(path: String): Array[String] = {
        val lsCmd =  hdfs + " dfs -ls " + "/jagathan/" + path
        val cmd: String = lsCmd.!!
        var fileNames = Array[String]()
        cmd.split("\n").drop(1).filter(!_.contains("SUCCESS")).foreach(line => {
            fileNames = fileNames :+ line.substring(line.lastIndexOf("/")+1)
        })
        fileNames
    }

    def getSize(path: String): Double = {
        val du = hdfs + " dfs -du -s /jagathan/" + path
        val res:String = du.!!
        val size = res.split("\\s+")(0).toLong
        val sizeInKb = size/1024.0
        val sizeInMb = sizeInKb/1024.0
        return sizeInMb
    }
    // def hdfsExists(spark: SparkSession, hdfsPath: String, filePath: String) = {
    //     val hadoopFs: FileSystem = FileSystem.get(new URI(hdfsPath), spark.sparkContext.hadoopConfiguration)
    //     testDirExist(filePath, hadoopFs)
    // }

    // def hdfsRename(spark: SparkSession, hdfsPath: String, filePath: String, newName: String) = {
    //     val hadoopFs: FileSystem = FileSystem.get(new URI(hdfsPath), spark.sparkContext.hadoopConfiguration)
    //     hadoopFs.rename(new Path(filePath + "part-00000"), new Path(filePath + newName))
    // }

    // def hdfsDelete(spark: SparkSession, hdfsPath: String, filePath: String) = {
    //     val hadoopFs: FileSystem = FileSystem.get(new URI(hdfsPath), spark.sparkContext.hadoopConfiguration)
    //     hadoopFs.delete(new Path(filePath), true)
    // }
}