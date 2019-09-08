package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object map_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val data_source: RDD[Int] = sc.parallelize(1 to 10)
    val map_RDD: RDD[Int] = data_source.map(_ * 2)
    println(map_RDD.collect().toBuffer)
    sc.stop()
  }
}
