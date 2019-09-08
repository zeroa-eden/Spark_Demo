package spark_rdd.itcast.cn.spark_rdd_action.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object takeSample_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd: RDD[Int] = sc.parallelize(1 to 10,2)
    val ts: Array[Int] = rdd.takeSample(true,5)
    println(ts.toBuffer)
  }
}
