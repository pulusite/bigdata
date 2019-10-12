package org.apache.spark.examples

/**
 * @Author: wei.hu
 * @CreateDate: 2019-08-22
 */
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * Author: wei.hu
 * CreateDate: 2018/10/18
 */
object WC {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org.apache.spark").setLevel(Level.INFO)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    val inputFile = "./data/word.txt";

    val conf = new SparkConf().setAppName("wordCount").setMaster("local[*]")

    val sc = new SparkContext(conf)


    val textFile = sc.textFile(inputFile)
    val wordCount = textFile
      .flatMap(line=>line.split("\\s+"))
      .map(w=>(w,1))
      .reduceByKey(_+_)
        .map(x=>(x._2,x._1)).sortByKey(false)

    wordCount.foreach(println)

    sc.stop()

  }

}
