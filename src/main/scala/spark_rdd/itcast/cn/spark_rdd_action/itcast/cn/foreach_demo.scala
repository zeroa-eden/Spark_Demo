package spark_rdd.itcast.cn.spark_rdd_action.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object foreach_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    var rdd = sc.makeRDD(1 to 10,2)
    var sum = sc.accumulator(0)
    println(rdd.foreach(sum+=_).toString)
    println( rdd.collect().foreach(println))
  }
}
