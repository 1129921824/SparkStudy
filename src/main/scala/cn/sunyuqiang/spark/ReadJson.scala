package cn.sunyuqiang.spark

import scala.util.parsing.json.JSON
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object ReadJson {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Coalesce").set("spark.testing.memory", "2147480000")
    val sc = new SparkContext(conf)
    val file: RDD[String] = sc.textFile("user.json")

    val result  = file.map(JSON.parseFull)

    result.foreach(println)

  }

}
