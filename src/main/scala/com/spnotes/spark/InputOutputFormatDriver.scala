package com.spnotes.spark

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.mapreduce.lib.input.{KeyValueTextInputFormat, TextInputFormat}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory
import org.apache.hadoop.io.{IntWritable, Text}

/**
  * Created by sunilpatil on 1/4/16.
  */
object InputOutputFormatDriver {
  val logger = Logger(LoggerFactory.getLogger("InputOutputFormatDriver"))

  def main (args: Array[String]) {
    logger.debug("Entering InputOutputFormatDriver.main")
    if (args.length != 2) {
      System.err.printf("Usage: %s [generic options] <input> <output>\n",
        getClass().getSimpleName());
      System.exit(-1);
    }

    val inputFile = args(0)
    val outputFile = args(1)
    logger.debug(s"Read json from $inputFile and write to $outputFile")

    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("InputOutputFormatDriver")
    val sparkContext = new SparkContext(sparkConf)

    //Configure the program to use Hadoop as input format and read from
    val lines = sparkContext.newAPIHadoopFile(inputFile,classOf[KeyValueTextInputFormat], classOf[Text],classOf[Text])
    val wordCountRDD = lines.keys.map(lineText => lineText.toString).flatMap(_.split(" ")).map(word => (word,1)).reduceByKey(_+_)

    // COnfigure the output to go to File with Text as key IntWritable as value using MapReduce Output Format
    wordCountRDD.saveAsNewAPIHadoopFile(outputFile,classOf[Text],classOf[IntWritable],classOf[TextOutputFormat[Text,IntWritable]])


    logger.debug("Exiting InputOutputFormatDriver.main")
  }

}
