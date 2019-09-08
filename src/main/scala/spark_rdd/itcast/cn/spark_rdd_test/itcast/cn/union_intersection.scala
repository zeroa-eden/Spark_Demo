package spark_rdd.itcast.cn.spark_rdd_test.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object union_intersection {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val data11: RDD[Int] = sc.parallelize(List(1,2,3))
    val data12: RDD[Int] = sc.parallelize(List(8,3,4,1))
    val union: RDD[Int] = data11.union(data12)
    println(union.collect().toBuffer)
    val intersection: RDD[Int] = data11.intersection(data12)
    println(intersection.collect().toBuffer)
  }
}
