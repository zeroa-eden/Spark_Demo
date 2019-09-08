package spark_rdd.itcast.cn.spark_rdd_action.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

object saveAsObjectFile {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd = sc.parallelize(Array(1,2,3))
    rdd.saveAsObjectFile("C:\\Users\\zero\\Desktop\\test\\obj")
  }
}
