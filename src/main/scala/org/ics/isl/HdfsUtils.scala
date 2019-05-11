package org.ics.isl

import java.io._
import sys.process._


object HdfsUtils {
    val hdfs = "hdfs"

    def hdfsExists(path: String): Boolean = {
		try {
      		val cmd = hdfs + " dfs -ls " + path
      		val output = cmd.!!
    	} catch {
      		case e: Exception => return false
    	}
    	return true
	}

    def moveFile(file: String, dest: String) = {
        val mkdir = hdfs + " dfs -mkdir " + dest
        val mkdirOut = mkdir ! 
        val cmd = hdfs + " dfs -mv " + file + " " + dest
        val output = cmd !
    }

	def removeDirInHDFS(path: String) = {
        val cmd = hdfs + " dfs -rm -r " + path
        val output = cmd !
    }

    def removeFilesInHDFS(path: String) = {
        val cmd = hdfs + " dfs -rm " + path
        val output = cmd !
    }

    def countSubFolders(path: String): Int = {
        val lsCmd =  hdfs + " dfs -ls " + path
        val cmd:String =  (lsCmd #| "wc -l").!!.trim
        (cmd.toInt - 2)
    }

    def getDirFilesNames(path: String): Array[String] = {
        val lsCmd =  hdfs + " dfs -ls " + path
        val cmd: String = lsCmd.!!
        var fileNames = Array[String]()
        cmd.split("\n").drop(1).filter(!_.contains("SUCCESS")).foreach(line => {
            fileNames = fileNames :+ line.substring(line.lastIndexOf("/")+1)
        })
        fileNames
    }

    def getSize(path: String): Double = {
        val du = hdfs + " dfs -du -s " + path
        val res:String = du.!!
        val size = res.split("\\s+")(0).toLong
        val sizeInKb = size/1024.0
        val sizeInMb = sizeInKb/1024.0
        return sizeInMb
    }

}