package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object flatmap_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("map")
    val sc = new SparkContext(conf)
    val data_RDD: RDD[Int] = sc.parallelize(1 to 10)
    val faltMap_RDD: RDD[Int] = data_RDD.flatMap(1 to _)
    println(faltMap_RDD.collect.toBuffer)
  }
}
