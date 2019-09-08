package spark_rdd.itcast.cn.spark_rdd_test.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object map_filter {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd  = sc.parallelize(List(1,2,3,4,5,6,7))
    val rdd2: RDD[Int] = rdd.map(_*2).sortBy(x=>x,true)
    val filter_rdd: RDD[Int] = rdd2.filter(_>5)
    println(filter_rdd.collect().toBuffer)
  }
}
