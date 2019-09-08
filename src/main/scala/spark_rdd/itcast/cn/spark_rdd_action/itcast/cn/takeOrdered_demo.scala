package spark_rdd.itcast.cn.spark_rdd_action.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object takeOrdered_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
   val rdd1: RDD[Int] = sc.makeRDD(Seq(10,4,2,12,3))
    println(rdd1.top(2).toBuffer)
    println(rdd1.takeOrdered(3).toBuffer)

  }
}
