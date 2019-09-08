package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用于过滤后减少分区
  */
object coalesce_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(1 to 16, 4)
    val coalesceRDD: RDD[Int] = rdd.coalesce(3)
    println(coalesceRDD.partitions.size)
  }
}
