package com.spnotes.spark

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory


object NetcatStreamClient{
  val logger = Logger(LoggerFactory.getLogger("NetcatStreamClient"))

  def main(argv:Array[String]): Unit ={
    logger.debug("Entering NetcatStreamClient.main")
    if(argv.length != 3){
      println("Please provide 3 parameters <host> <port> <microbatchtime>")
      System.exit(1)
    }
    val hostName =argv(0)
    val port = argv(1).toInt
    val microBatchTime = argv(2).toInt

    logger.debug(s"Listening on $hostName at $port batching records every $microBatchTime")

    //Create Spark Configuration
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sparkStreamingContext = new StreamingContext(sparkConf,Seconds(microBatchTime))

    val lines = sparkStreamingContext.socketTextStream(hostName,port)
    lines.flatMap(_.split(" ")).map(word => (word,1)).reduceByKey(_+_).print()
    logger.debug("Number of words " + lines.count())

    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()
    logger.debug("Exiting NetcatStreamClient.main")

  }
}