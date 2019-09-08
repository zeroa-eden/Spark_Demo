package spark_rdd.itcast.cn.spark_rdd_transformation.itcast.cn

import org.apache.spark.{SparkConf, SparkContext}

/**
  * cartesian笛卡儿积
  */
object cartesian_demo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("sample")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val rdd1 = sc.parallelize(1 to 3)
    val rdd2 = sc.parallelize(2 to 5)
    println(rdd1.cartesian(rdd2).collect().toBuffer)
  }
}
