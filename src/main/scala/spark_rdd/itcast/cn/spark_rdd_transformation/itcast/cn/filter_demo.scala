package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object filter_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val data_RDD: RDD[Int] = sc.parallelize(1 to 10)
    val filter_RDD: RDD[Int] = data_RDD.filter( _ > 3)
    println(filter_RDD.collect().toBuffer)
  }
}
