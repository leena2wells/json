package com.spnotes.spark

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.Logger
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory


class Person {
  @JsonProperty var first: String = null
  @JsonProperty var last: String = null
  @JsonProperty var address: Address = null

  override def toString = s"Person(first=$first, last=$last, address=$address)"
}

class Address {
  @JsonProperty var line1: String = null
  @JsonProperty var line2: String = null
  @JsonProperty var city: String = null
  @JsonProperty var state: String = null
  @JsonProperty var zip: String = null

  override def toString = s"Address(line1=$line1, line2=$line2, city=$city, state=$state, zip=$zip)"
}

object JSONFileReaderWriter {
  val logger = Logger(LoggerFactory.getLogger("JSONFileReaderWriter"))
  val mapper = new ObjectMapper()

  def main(argv: Array[String]): Unit = {

    if (argv.length != 2) {
      println("Please provide 2 parameters <inputfile> <outputfile>")
      System.exit(1)
    }
    val inputFile = argv(0)
    val outputFile = argv(1)

    logger.debug(s"Read json from $inputFile and write to $outputFile")

    val sparkConf = new SparkConf().setMaster("local[1]").setAppName("JSONFileReaderWriter")
    val sparkContext = new SparkContext(sparkConf)
    val errorRecords = sparkContext.accumulator(0)

    val records = sparkContext.textFile(inputFile)

    var results = records.flatMap { record =>
      try {
        Some(mapper.readValue(record, classOf[Person]))
      } catch {
        case e: Exception => {
          errorRecords += 1
          None
        }
      }
    }.filter(person => person.address.city.equals("mumbai")).saveAsTextFile(outputFile)

    println("Number of bad records " + errorRecords)
  }
}
