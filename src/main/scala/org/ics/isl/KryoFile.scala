package org.ics.isl

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Input
import org.apache.hadoop.io.{BytesWritable, NullWritable}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag


object KryoFile {
	/*
   	* Used to write as Object file using kryo serialization
   	*/
	def saveAsObjectFile[T: ClassTag](rdd: RDD[T], path: String) {
	    val kryoSerializer = new KryoSerializer(rdd.context.getConf)

	    rdd.mapPartitions(iter => iter.grouped(10)
		      .map(_.toArray))
		      .map(splitArray => {
	      //initializes kyro and calls your registrator class
	    val kryo = kryoSerializer.newKryo()

	      //convert data to bytes
	    val bao = new ByteArrayOutputStream()
	    val output = kryoSerializer.newKryoOutput()
	    output.setOutputStream(bao)
	    kryo.writeClassAndObject(output, splitArray)
	    output.close()

	      // We are ignoring key field of sequence file
	    val byteWritable = new BytesWritable(bao.toByteArray)
	      	(NullWritable.get(), byteWritable)
	    }).saveAsSequenceFile(path)
  	}

  	/*
	 * Method to read from object file which is saved kryo format.
	 */
  	def objectFile[T](sc: SparkContext, path: String, minPartitions: Int = 1)(implicit ct: ClassTag[T]) = {
	    val kryoSerializer = new KryoSerializer(sc.getConf)

	    sc.sequenceFile(path, classOf[NullWritable], classOf[BytesWritable], minPartitions)
	      .flatMap(x => {
				      val kryo = kryoSerializer.newKryo()
				      val input = new Input()
				      input.setBuffer(x._2.getBytes)
				      val data = kryo.readClassAndObject(input)
				      val dataObject = data.asInstanceOf[Array[T]]
				      dataObject
	    })

	}
}
