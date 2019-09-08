package spark_rdd.itcast.cn.spark_rdd_test.itcast.cn

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object flatamap {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val data = sc.parallelize(Array("a,b,x","huahua,xiai"))
    val flatmap: RDD[String] = data.flatMap(_.split(","))
   println(flatmap.collect().toBuffer)
  }
}
