package com.firearasi
import org.apache.spark.{SparkContext,SparkConf}
import org.apache.spark.SparkContext._


object WordCount  {
  def main(args: Array[String]) {

    if(args.length<1) {
      System.err.println("Not enough args!")

      System.exit(1)
    }


    val conf=new SparkConf().setAppName("FirstSpark")
    val sc=new SparkContext(conf)


    println("Printing ten highest words of "+args(0))


    val start = System.currentTimeMillis

    val rawTf=sc.textFile(args(0))

    //println("rawTF")
    //rawTf.collect.foreach(println)

    val pairs=rawTf.flatMap(_.toLowerCase.split("\\s+")).flatMap(_.split(",")).flatMap(_.split("\\")).flatMap(_.split("/")).filter(_!="").map((_,1))

    //println("pairs")
    //pairs.collect.foreach(println)


    val wc=pairs.reduceByKey(_+_)

    println("wc")
    wc.take(10).foreach(println)


    val end= System.currentTimeMillis
    println("Elapsed time:"+(end-start).toString+" milliseconds")
  }
}