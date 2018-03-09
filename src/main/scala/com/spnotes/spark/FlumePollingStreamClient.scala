package com.spnotes.spark

import com.typesafe.scalalogging.Logger
import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory


object FlumePollingStreamClient {
  val logger = Logger(LoggerFactory.getLogger("FlumePollingStreamClient"))


  def main(argv:Array[String]): Unit ={
    logger.debug("Entering FlumePollingStreamClient.main")

    if(argv.length != 3){
      println("Please provide 3 parameters <host> <port> <microbatchtime>")
      System.exit(1)
    }
    val hostName =argv(0)
    val port = argv(1).toInt
    val microBatchTime = argv(2).toInt

    logger.debug(s"Listening on $hostName at $port batching records every $microBatchTime")

    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val sparkStreamingContext = new StreamingContext(sparkConf,Seconds(microBatchTime))

    val stream = FlumeUtils.createPollingStream(sparkStreamingContext,hostName,port)
 //   val lines = FlumeUtils.createStream(sparkStreamingContext,hostName,port)

    val mappedlines = stream.map{ sparkFlumeEvent =>
      val event = sparkFlumeEvent.event
      println("Value of event " + event)
      println("Value of event Header " + event.getHeaders)
      println("Value of event Schema " + event.getSchema)
      val messageBody = new String(event.getBody.array())
      println("Value of event Body " + messageBody)
      messageBody
    }.print()


    stream.count().map(cnt => "Received " + cnt + " flume events." ).print()
    sparkStreamingContext.start()
    sparkStreamingContext.awaitTermination()

    logger.debug("Exiting FlumePollingStreamClient.main")

  }
}
