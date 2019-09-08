package spark_rdd.itcast.cn.spark_rdd_test.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object reduce {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1: RDD[Int] = sc.parallelize(List(1,2,3,4,5))
    val reduce: Int = rdd1.reduce(_+_)
    println(reduce)
  }
}
