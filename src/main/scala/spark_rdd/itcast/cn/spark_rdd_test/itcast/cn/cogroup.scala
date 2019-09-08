package spark_rdd.itcast.cn.spark_rdd_test.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object cogroup {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("jim", 2)))
    //cogroup
    val rdd3 = rdd1.cogroup(rdd2)
    //注意cogroup与groupByKey的区别
    println(rdd3.collect.toBuffer)
  }

}
